package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/pkg/browser"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/gookit/color"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	k8sTypesMetaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sVersion "k8s.io/apimachinery/pkg/version"
	k8sClientCoreV1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"

	"github.com/datawire/ambassador/pkg/k8s"
	"github.com/datawire/ambassador/pkg/supervisor"

	"github.com/datawire/ambassador/pkg/helm"
)

const (
	// defInstallNamespace is the default installation namespace
	defInstallNamespace = "ambassador"

	// env variable used for specifying an alternative Helm repo
	defEnvVarHelmRepo = "AES_HELM_REPO"

	// env variable used for specifying a SemVer for whitelisting Charts
	// For example, '1.3.*' will install the latest Chart from the Helm repo that installs
	// an image with a '1.3.*' tag.
	defEnvVarChartVersionRule = "AES_CHART_VERSION"

	// env variable used for specifying the image repository (ie, 'quay.io/datawire/aes')
	// this will install the latest Chart from the Helm repo, but with an overridden `image.repository`
	defEnvVarImageRepo = "AES_IMAGE_REPOSITORY"

	// env variable used for overriding the image tag (ie, '1.3.2')
	// this will install the latest Chart from the Helm repo, but with an overridden `image.tag`
	defEnvVarImageTag = "AES_IMAGE_TAG"
)

var (
	// defChartValues defines some default values for the Helm chart
	// see https://github.com/datawire/ambassador-chart#configuration
	defChartValues = map[string]string{
		"replicas":       "1",
		"deploymentTool": "edgectl", // undocumented value, used for setting the "app.kubernetes.io/managed-by"
		"namespace.name": defInstallNamespace,
	}

	// defEdgectlInstallLabel is the label of the deployments installed by Helm
	defEdgectlInstallLabel = "app.kubernetes.io/managed-by=edgectl"

	// defInstallationFingerprintLabels is a list of labels that can be used
	// for identifying an Ambassador installation (and the mechanism used for installing it)
	defInstallationFingerprintLabels = []struct {
		Label  string
		Method string
	}{
		{defEdgectlInstallLabel, "edgectl"},
		{"app.kubernetes.io/managed-by=amb-oper", "operator"},
		{"app.kubernetes.io/name=ambassador", "helm"},
		{"product=aes", "aes"},
		{"service=ambassador", "oss"},
	}
)

func aesInstallCmd() *cobra.Command {
	res := &cobra.Command{
		Use:   "install",
		Short: "Install the Ambassador Edge Stack in your cluster",
		Args:  cobra.ExactArgs(0),
		RunE:  aesInstall,
	}
	_ = res.Flags().StringP(
		"context", "c", "",
		"The Kubernetes context to use. Defaults to the current kubectl context.",
	)
	_ = res.Flags().BoolP(
		"verbose", "v", false,
		"Show all output. Defaults to sending most output to the logfile.",
	)
	return res
}

func getEmailAddress(defaultEmail string, log *log.Logger) string {
	prompt := fmt.Sprintf("Email address [%s]: ", defaultEmail)
	errorFallback := defaultEmail
	if defaultEmail == "" {
		prompt = "Email address: "
		errorFallback = "email_query_failure@datawire.io"
	}

	for {
		fmt.Print(prompt)
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		text := scanner.Text()
		if err := scanner.Err(); err != nil {
			log.Printf("Email query failed: %+v", err)
			return errorFallback
		}

		text = strings.TrimSpace(text)
		if defaultEmail != "" && text == "" {
			return defaultEmail
		}

		if validEmailAddress.MatchString(text) {
			return text
		}

		fmt.Printf("Sorry, %q does not appear to be a valid email address.  Please check it and try again.\n", text)
	}
}

func aesInstall(cmd *cobra.Command, args []string) error {
	skipReport, _ := cmd.Flags().GetBool("no-report")
	verbose, _ := cmd.Flags().GetBool("verbose")
	kcontext, _ := cmd.Flags().GetString("context")
	i := NewInstaller(verbose)

	// If Scout is disabled (environment variable set to non-null), inform the user.
	if i.scout.Disabled() {
		i.show.Printf(phoneHomeDisabled)
	}

	// Both printed and logged when verbose (Installer.log is responsible for --verbose)
	i.log.Printf(fmt.Sprintf(installAndTraceIDs, i.scout.installID, i.scout.metadata["trace_id"]))

	sup := supervisor.WithContext(i.ctx)
	sup.Logger = i.log

	sup.Supervise(&supervisor.Worker{
		Name: "signal",
		Work: func(p *supervisor.Process) error {
			sigs := make(chan os.Signal, 1)
			signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
			p.Ready()
			select {
			case sig := <-sigs:
				i.Report("user_interrupted", ScoutMeta{"signal", fmt.Sprintf("%+v", sig)})
				i.Quit()
			case <-p.Shutdown():
			}
			return nil
		},
	})
	sup.Supervise(&supervisor.Worker{
		Name:     "install",
		Requires: []string{"signal"},
		Work: func(p *supervisor.Process) error {
			defer i.Quit()
			result := i.Perform(kcontext)
			i.ShowResult(result)
			return result.Err
		},
	})

	// Don't allow messages emitted while opening the browser to mess up our
	// carefully-crafted terminal output
	browser.Stdout = ioutil.Discard
	browser.Stderr = ioutil.Discard

	runErrors := sup.Run()
	if len(runErrors) > 1 { // This shouldn't happen...
		for _, err := range runErrors {
			i.show.Printf(err.Error())
		}
	}
	if len(runErrors) > 0 {
		if !skipReport {
			i.generateCrashReport(runErrors[0])
		}
		i.show.Println()
		i.show.Printf("Full logs at %s\n\n", i.logName)
		return runErrors[0]
	}
	return nil
}

// LoopFailedError is a fatal error for loopUntil(...)
type LoopFailedError string

// Error implements error
func (s LoopFailedError) Error() string {
	return string(s)
}

type loopConfig struct {
	sleepTime    time.Duration // How long to sleep between calls
	progressTime time.Duration // How long until we explain why we're waiting
	timeout      time.Duration // How long until we give up
}

var lc2 = &loopConfig{
	sleepTime:    500 * time.Millisecond,
	progressTime: 15 * time.Second,
	timeout:      120 * time.Second,
}

var lc5 = &loopConfig{
	sleepTime:    3 * time.Second,
	progressTime: 30 * time.Second,
	timeout:      5 * time.Minute,
}

// loopUntil repeatedly calls a function until it succeeds, using a
// (presently-fixed) loop period and timeout.
func (i *Installer) loopUntil(what string, how func() error, lc *loopConfig) error {
	ctx, cancel := context.WithTimeout(i.ctx, lc.timeout)
	defer cancel()
	start := time.Now()
	i.log.Printf("Waiting for %s", what)
	defer func() { i.log.Printf("Wait for %s took %.1f seconds", what, time.Since(start).Seconds()) }()
	progTimer := time.NewTimer(lc.progressTime)
	defer progTimer.Stop()
	for {
		err := how()
		if err == nil {
			return nil // Success
		} else if _, ok := err.(LoopFailedError); ok {
			return err // Immediate failure
		}
		// Wait and try again
		select {
		case <-progTimer.C:
			i.show.Printf("   Still waiting for %s. (This may take a minute.)", what)
		case <-time.After(lc.sleepTime):
			// Try again
		case <-ctx.Done():
			return errors.Errorf("timed out waiting for %s (or interrupted)", what)
		}
	}
}

// GrabAESInstallID uses "kubectl exec" to ask the AES pod for the cluster's ID,
// which we uses as the AES install ID. This has the side effect of making sure
// the Pod is Running (though not necessarily Ready). This should be good enough
// to report the "deploy" status to metrics.
func (i *Installer) GrabAESInstallID() error {
	aesImage := "quay.io/datawire/aes:" + i.version
	podName := ""
	containerName := ""
	podInterface := i.coreClient.Pods("ambassador") // namespace
	i.log.Print("> k -n ambassador get po")
	pods, err := podInterface.List(k8sTypesMetaV1.ListOptions{})
	if err != nil {
		return err
	}

	// Find an AES Pod
PodsLoop:
	for _, pod := range pods.Items {
		i.log.Print("  Pod: ", pod.Name)
	ContainersLoop:
		for _, container := range pod.Spec.Containers {
			// Avoid matching the Traffic Manager (container.Command == ["traffic-proxy"])
			i.log.Printf("       Container: %s (image: %q; command: %q)", container.Name, container.Image, container.Command)
			if container.Image != aesImage || container.Command != nil {
				continue
			}
			// Avoid matching the Traffic Agent by checking for
			// AGENT_SERVICE in the environment. This is how Ambassador's
			// Python code decides it is running as an Agent.
			for _, envVar := range container.Env {
				if envVar.Name == "AGENT_SERVICE" && envVar.Value != "" {
					i.log.Printf("                  AGENT_SERVICE: %q", envVar.Value)
					continue ContainersLoop
				}
			}
			i.log.Print("       Success")
			podName = pod.Name
			containerName = container.Name
			break PodsLoop
		}
	}
	if podName == "" {
		return errors.New("no AES pods found")
	}

	// Retrieve the cluster ID
	clusterID, err := i.CaptureKubectl("get cluster ID", "", "-n", "ambassador", "exec", podName, "-c", containerName, "python3", "kubewatch.py")
	if err != nil {
		return err
	}
	i.clusterID = clusterID
	i.SetMetadatum("Cluster ID", "aes_install_id", clusterID)
	return nil
}

// GrabLoadBalancerAddress retrieves the AES service load balancer's address (IP
// address or hostname)
func (i *Installer) GrabLoadBalancerAddress() error {
	serviceInterface := i.coreClient.Services(defInstallNamespace) // namespace
	service, err := serviceInterface.Get("ambassador", k8sTypesMetaV1.GetOptions{})
	if err != nil {
		return err
	}
	for _, ingress := range service.Status.LoadBalancer.Ingress {
		if net.ParseIP(ingress.IP) != nil {
			i.address = ingress.IP
			return nil
		}
		if ingress.Hostname != "" {
			i.address = ingress.Hostname
			return nil
		}
	}
	return errors.New("no address found")
}

// CheckAESServesACME performs the same checks that the edgestack.me name
// service performs against the AES load balancer host
func (i *Installer) CheckAESServesACME() (err error) {
	defer func() {
		if err != nil {
			i.log.Print(err.Error())
		}
	}()

	// Verify that we can connect to something
	resp, err := http.Get("http://" + i.address + "/.well-known/acme-challenge/")
	if err != nil {
		err = errors.Wrap(err, "check for AES")
		return
	}
	_, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	// Verify that we get the expected status code. If Ambassador is still
	// starting up, then Envoy may return "upstream request timeout" (503),
	// in which case we should keep looping.
	if resp.StatusCode != 404 {
		err = errors.Errorf("check for AES: wrong status code: %d instead of 404", resp.StatusCode)
		return
	}

	// Sanity check that we're talking to Envoy. This is probably unnecessary.
	if resp.Header.Get("server") != "envoy" {
		err = errors.Errorf("check for AES: wrong server header: %s instead of envoy", resp.Header.Get("server"))
		return
	}

	return nil
}

// CheckAESHealth retrieves AES's idea of whether it is healthy, i.e. ready.
func (i *Installer) CheckAESHealth() error {
	resp, err := http.Get("https://" + i.hostname + "/ambassador/v0/check_ready")
	if err != nil {
		return err
	}
	_, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != 200 {
		return errors.Errorf("check for AES health: wrong status code: %d instead of 200", resp.StatusCode)
	}

	return nil
}

// CheckHostnameFound tries to connect to check-blah.hostname to see whether DNS
// has propagated. Each connect talks to a different hostname to try to avoid
// NXDOMAIN caching.
func (i *Installer) CheckHostnameFound() error {
	conn, err := net.Dial("tcp", fmt.Sprintf("check-%d.%s:443", time.Now().Unix(), i.hostname))
	if err == nil {
		conn.Close()
	}
	return err
}

// CheckACMEIsDone queries the Host object and succeeds if its state is Ready.
func (i *Installer) CheckACMEIsDone() error {
	state, err := i.CaptureKubectl("get Host state", "", "get", "host", i.hostname, "-o", "go-template={{.status.state}}")
	if err != nil {
		return LoopFailedError(err.Error())
	}
	if state == "Error" {
		reason, err := i.CaptureKubectl("get Host error", "", "get", "host", i.hostname, "-o", "go-template={{.status.errorReason}}")
		if err != nil {
			return LoopFailedError(err.Error())
		}
		// This heuristic tries to detect whether the error is that the ACME
		// provider got NXDOMAIN for the provided hostname. It specifically
		// handles the error message returned by Let's Encrypt in Feb 2020, but
		// it may cover others as well. The AES ACME controller retries much
		// sooner if this heuristic is tripped, so we should continue to wait
		// rather than giving up.
		isAcmeNxDomain := strings.Contains(reason, "NXDOMAIN") || strings.Contains(reason, "urn:ietf:params:acme:error:dns")
		if isAcmeNxDomain {
			return errors.New("Waiting for NXDOMAIN retry")
		}

		// TODO: Windows incompatible, will not be bold but otherwise functions.
		// TODO: rewrite Installer.show to make explicit calls to color.Bold.Printf(...) instead,
		// TODO: along with logging.  Search for color.Bold to find usages.

		i.show.Println()
		i.show.Println(color.Bold.Sprintf("Acquiring TLS certificate via ACME has failed: %s", reason))
		return LoopFailedError(fmt.Sprintf("ACME failed. More information: kubectl get host %s -o yaml", i.hostname))
	}
	if state != "Ready" {
		return errors.Errorf("Host state is %s, not Ready", state)
	}
	return nil
}

// CreateNamespace creates the namespace for installing AES
func (i *Installer) CreateNamespace() error {
	i.CaptureKubectl("create namespace", "", "create", "namespace", defInstallNamespace)
	// ignore errors: it will fail if the namespace already exists
	// TODO: check that the error message contains "already exists"
	return nil
}

// GetExistingInstallation tries to find an existing deployment by looking at a list of predefined labels,
// If such a deployment is found, it returns the image and the installation "family" (aes, oss, helm, etc).
// It returns an empty string if no installation could be found.
//
// TODO: Try to search all namespaces (which may fail due to RBAC) and capture a
//       correct namespace for an Ambassador installation (what if there is more than
//       one?), then proceed operating on that Ambassador in that namespace. Right now
//       we hard-code the "ambassador" namespace in a number of spots.
func (i *Installer) GetExistingInstallation() (string, string, error) {
	findForLabel := func(label string) (string, error) {
		aesVersionRE := regexp.MustCompile("quay[.]io/datawire/aes:([[:^space:]]+)")
		deploys, err := i.CaptureKubectl("get AES deployment", "",
			"-n", defInstallNamespace,
			"get", "deploy",
			"-l", label,
			"-o", "go-template='{{range .items}}{{range .spec.template.spec.containers}}{{.image}}\n{{end}}{{end}}'")
		if err != nil {
			return "", err
		}
		scanner := bufio.NewScanner(strings.NewReader(deploys))
		for scanner.Scan() {
			image := strings.TrimSpace(scanner.Text())
			if matches := aesVersionRE.FindStringSubmatch(image); len(matches) == 2 {
				return matches[1], nil
			}
		}
		return "", scanner.Err()
	}

	for _, installation := range defInstallationFingerprintLabels {
		version, err := findForLabel(installation.Label)
		if err != nil {
			continue // ignore errors
		}
		if version != "" {
			return version, installation.Method, nil
		}
	}
	return "", "", nil
}

// Perform is the main function for the installer
func (i *Installer) Perform(kcontext string) Result {
	chartValues := map[string]string{}
	for k, v := range defChartValues {
		chartValues[k] = v
	}

	// Start
	i.Report("install")

	i.show.Println()
	i.show.Println(color.Bold.Sprintf(welcomeInstall))

	// Attempt to grab a reasonable default for the user's email address
	defaultEmail, err := i.Capture("get email", true, "", "git", "config", "--global", "user.email")
	if err != nil {
		i.log.Print(err)
		defaultEmail = ""
	} else {
		defaultEmail = strings.TrimSpace(defaultEmail)
		if !validEmailAddress.MatchString(defaultEmail) {
			defaultEmail = ""
		}
	}

	// Ask for the user's email address
	i.show.Println()
	i.ShowWrapped(emailAsk)
	// Do the goroutine dance to let the user hit Ctrl-C at the email prompt
	gotEmail := make(chan string)
	var emailAddress string
	go func() {
		gotEmail <- getEmailAddress(defaultEmail, i.log)
		close(gotEmail)
	}()
	select {
	case emailAddress = <-gotEmail:
		// Continue
	case <-i.ctx.Done():
		fmt.Println()
		return UnhandledErrResult(errors.New("Interrupted"))
	}
	i.show.Println()

	i.log.Printf("Using email address %q", emailAddress)

	i.show.Println("========================================================================")
	i.show.Println(beginningAESInstallation)
	i.show.Println()

	// Attempt to use kubectl
	_, err = i.GetKubectlPath()
	// err = errors.New("early error for testing")  // TODO: remove for production
	if err != nil {
		i.Report("fail_no_kubectl")
		err = browser.OpenURL(noKubectlURL)
		return UnhandledErrResult(fmt.Errorf(noKubectl))
	}

	// Attempt to talk to the specified cluster
	i.kubeinfo = k8s.NewKubeInfo("", kcontext, "")
	if err := i.ShowKubectl("cluster-info", "", "cluster-info"); err != nil {
		i.Report("fail_no_cluster")
		err = browser.OpenURL(noClusterURL)
		return UnhandledErrResult(fmt.Errorf(noCluster))
	}
	i.restConfig, err = i.kubeinfo.GetRestConfig()
	if err != nil {
		i.Report("fail_no_cluster")
		return UnhandledErrResult(err)
	}
	i.coreClient, err = k8sClientCoreV1.NewForConfig(i.restConfig)
	if err != nil {
		i.Report("fail_no_cluster")
		return UnhandledErrResult(err)
	}

	versions, err := i.CaptureKubectl("get versions", "", "version", "-o", "json")
	if err != nil {
		i.Report("fail_no_cluster")
		return UnhandledErrResult(err)
	}
	kubernetesVersion := &kubernetesVersion{}
	err = json.Unmarshal([]byte(versions), kubernetesVersion)
	if err != nil {
		// We tried to extract Kubernetes client and server versions but failed.
		// This should not happen since we already validated the cluster-info, but still...
		// It's not critical if this information is missing, other than for debugging purposes.
		i.log.Printf("failed to read Kubernetes client and server versions: %v", err.Error())
	}
	i.k8sVersion = kubernetesVersion
	// Metriton tries to parse fields with `version` in their keys and discards them if it can't.
	// Using _v to keep the version value as string since Kubernetes versions vary in formats.
	i.SetMetadatum("kubectl Version", "kubectl_v", i.k8sVersion.Client.GitVersion)
	i.SetMetadatum("K8s Version", "k8s_v", i.k8sVersion.Server.GitVersion)

	// Try to grab some cluster info
	if err := i.UpdateClusterInfo(); err != nil {
		i.ShowWrapped(fmt.Sprintf("-> Failed to get some cluster information"), seeDocs)
		i.Report("fail_cluster_info", ScoutMeta{"err", err.Error()})
		return UnhandledErrResult(errors.Wrap(err, "could not get cluster info"))
	}
	i.SetMetadatum("Cluster Info", "cluster_info", i.clusterinfo.name)

	// Try to verify the existence of an Ambassador deployment in the Ambassador
	// namespace.
	installedVersion, installedFamily, err := i.GetExistingInstallation()
	if err != nil {
		i.show.Println("Failed to look for an existing installation:", err)
		installedVersion = "" // Things will likely fail when we try to apply manifests
	}

	// the Helm chart heuristics look for the latest release that matches `version_rule`
	version_rule := "*"
	if vr := os.Getenv(defEnvVarChartVersionRule); vr != "" {
		i.ShowWrapped(fmt.Sprintf("Overriding Chart version rule from %q: %s.", defEnvVarChartVersionRule, vr))
		version_rule = vr
	} else {
		// Allow overriding the image repo and tag
		// This is mutually exclusive with the Chart version rule: it would be too messy otherwise.
		if ir := os.Getenv(defEnvVarImageRepo); ir != "" {
			i.ShowWrapped(fmt.Sprintf("Overriding image repo from %q: %s.", defEnvVarImageRepo, ir))
			chartValues["image.repository"] = ir
		}

		if it := os.Getenv(defEnvVarImageTag); it != "" {
			i.ShowWrapped(fmt.Sprintf("Overriding image tag from %q: %s.", defEnvVarImageTag, it))
			chartValues["image.tag"] = it
		}
	}

	// create a new parsed checker for versions
	chartVersion, err := helm.NewChartVersionRule(version_rule)
	if err != nil {
		i.Report("fail_no_internet", ScoutMeta{"err", err.Error()})
		return UnhandledErrResult(errors.Wrap(err, "download AES CRD manifests"))
	}

	mngr, err := manager.New(i.restConfig, manager.Options{})
	if err != nil {
		i.Report("fail_creating_downloader", ScoutMeta{"err", err.Error()})
		return UnhandledErrResult(err)
	}

	helmDownloaderOptions := helm.HelmDownloaderOptions{
		Version: chartVersion,
		Logger:  i.log,
		Manager: mngr,
	}
	if u := os.Getenv(defEnvVarHelmRepo); u != "" {
		i.ShowWrapped(fmt.Sprintf("Overriding Helm repo from %q: %s.", defEnvVarHelmRepo, u))
		helmDownloaderOptions.URL = u
	}

	// create a new manager for the remote Helm repo URL
	chartDown, err := helm.NewHelmDownloader(helmDownloaderOptions)
	if err != nil {
		i.Report("fail_creating_downloader", ScoutMeta{"err", err.Error()})
		return UnhandledErrResult(err)
	}

	i.ShowWrapped(fmt.Sprintf("-> Checking latest version of Ambassador Edge Stack available..."))

	if err := chartDown.Download(); err != nil {
		i.Report("fail_no_internet", ScoutMeta{"err", err.Error()})
		return UnhandledErrResult(errors.Wrap(err, "download AES CRD manifests"))
	}

	chartRel, err := chartDown.GetReleaseMgr(defInstallNamespace, defChartValues)
	defer func() { _ = chartDown.Cleanup() }()

	// the AES version we have downloaded
	i.version = strings.Trim(chartDown.GetChart().AppVersion, "\n")

	alreadyInstalled := false
	if installedVersion != "" {
		alreadyInstalled = true
	} else {
		i.ShowWrapped(fmt.Sprintf("-> Checking previous installations..."))
		if err := chartRel.Sync(i.ctx); err != nil {
			i.log.Printf("Failed to sync release: %s", err)
			i.Report("fail_download", ScoutMeta{"err", err.Error()})
			return UnhandledErrResult(err)
		}

		if chartRel.IsInstalled() {
			// There is an Ambassador Helm chart installed. Don't do anything.
			i.ShowWrapped("-> Ambassador Edge Stack was already installed with this tool.")
			alreadyInstalled = true
			installedFamily = "edgectl"
		}
	}

	if alreadyInstalled {
		i.ShowWrapped(fmt.Sprintf("-> Found an existing installation of Ambassador Edge Stack %s [%s].", installedVersion, installedFamily))
		i.Report("deploy", ScoutMeta{"already_installed", true})

		switch installedFamily {
		case "oss", "aes", "edgectl", "operator":
			i.ShowWrapped(abortExisting, seeDocs)
			i.SetMetadatum("Cluster Info", "managed", installedFamily)
			i.Report("fail_existing_oss",
				ScoutMeta{"installing", i.version},
				ScoutMeta{"found", installedVersion})
			return UnhandledErrResult(errors.Errorf("existing AES %s found when installing AES %s", installedVersion, i.version))

		case "helm":
			i.ShowWrapped("Ambassador has been installed with Helm.")
			i.SetMetadatum("Cluster Info", "managed", "helm")
			i.Report("existing_helm",
				ScoutMeta{"installing", i.version},
				ScoutMeta{"found", installedVersion})

		default:
			// any other case: continue with the rest of the setup
		}
	} else {
		// Ambassador is definetively not installed: perform the installation
		i.SetMetadatum("AES version being installed", "aes_version", i.version)
		i.ShowWrapped(fmt.Sprintf("-> Installing the Ambassador Edge Stack %s.", i.version))

		err = i.CreateNamespace()
		if err != nil {
			i.ShowWrapped(fmt.Sprintf("Namespace creation failed: %s", err))
			i.Report("fail_install_aes", ScoutMeta{"err", err.Error()})
			return UnhandledErrResult(err)
		}
		installedRelease, err := chartRel.InstallRelease(i.ctx)
		if err != nil {
			msg := fmt.Sprintf("Installation of a release failed: %s", err)
			if installedRelease != nil {
				msg += fmt.Sprintf(" (version %s)", installedRelease.Chart.AppVersion())
			}
			i.ShowWrapped(msg)
			if ir := os.Getenv("DEBUG"); ir != "" {
				i.ShowWrapped(installedRelease.Info.Notes)
			}
			i.Report("fail_install_aes", ScoutMeta{"err", err.Error()})
			return UnhandledErrResult(err)
		}

		i.ShowWrapped(fmt.Sprintf("-> Installed Ambassador Edge Stack %s", installedRelease.Chart.AppVersion()))
		i.ShowWrapped("-> Waiting for Ambassador Edge Stack to be ready...")
		if err := i.ShowKubectl("wait for AES", "",
			"-n", defInstallNamespace,
			"wait", "--for", "condition=available",
			"--timeout=90s",
			"deploy", "-l", "product=aes"); err != nil {
			i.Report("fail_wait_aes")
			return UnhandledErrResult(err)
		}
	}

	// Wait for Ambassador Pod; grab AES install ID
	i.show.Println("-> Checking the AES pod deployment")
	if err := i.loopUntil("AES pod startup", i.GrabAESInstallID, lc2); err != nil {
		i.Report("fail_pod_timeout")
		return UnhandledErrResult(err)
		i.ShowWrapped("-> New release installed successfully.")
		//if installedRelease.Info != nil {
		//	// show the Helm info message
		//	// TODO: maybe we could print some part of this message
		//	i.ShowWrapped(installedRelease.Info.Notes)
		//}
		i.Report("deploy", ScoutMeta{"already_installed", false})
	}

	// Don't proceed any further if we know we are using a local (not publicly
	// accessible) cluster. There's no point wasting the user's time on
	// timeouts.

	if i.clusterinfo.isLocal {
		i.Report("cluster_not_accessible")
		i.show.Println("-> Local cluster detected. Not configuring automatic TLS.")
		i.show.Println()
		i.ShowWrapped(color.Bold.Sprintf(noTlsSuccess))
		i.show.Println()
		loginMsg := "Determine the IP address and port number of your Ambassador service, e.g.\n"
		loginMsg += color.Bold.Sprintf("$ minikube service -n ambassador ambassador\n\n")
		loginMsg += fmt.Sprintf(loginViaIP)
		loginMsg += color.Bold.Sprintf("$ edgectl login -n ambassador IP_ADDRESS:PORT")
		i.ShowWrapped(loginMsg)
		i.show.Println()
		i.ShowWrapped(seeDocs)
		return UnhandledErrResult(nil)
	}

	// Grab load balancer address
	i.show.Println("-> Provisioning a cloud load balancer")
	if err := i.loopUntil("Load Balancer", i.GrabLoadBalancerAddress, lc5); err != nil {
		i.Report("fail_loadbalancer_timeout")
		i.show.Println()
		i.ShowWrapped(failLoadBalancer)
		i.show.Println()
		i.ShowWrapped(color.Bold.Sprintf(noTlsSuccess))
		i.ShowWrapped(seeDocs)
		return UnhandledErrResult(err)
	}
	i.Report("cluster_accessible")
	i.show.Println("-> Your AES installation's address is", color.Bold.Sprintf(i.address))

	// Wait for Ambassador to be ready to serve ACME requests.
	i.show.Println("-> Checking that AES is responding to ACME challenge")
	if err := i.loopUntil("AES to serve ACME", i.CheckAESServesACME, lc2); err != nil {
		i.Report("aes_listening_timeout")
		i.ShowWrapped("It seems AES did not start in the expected time, or the AES load balancer is not reachable from here.")
		i.ShowWrapped(tryAgain)
		i.ShowWrapped(color.Bold.Sprintf(noTlsSuccess))
		i.ShowWrapped(seeDocs)
		return UnhandledErrResult(err)
	}
	i.Report("aes_listening")

	i.show.Println("-> Automatically configuring TLS")

	// Send a request to acquire a DNS name for this cluster's load balancer
	regURL := "https://metriton.datawire.io/register-domain"
	regData := &registration{Email: emailAddress}
	if !i.scout.Disabled() {
		regData.AESInstallId = i.clusterID
		regData.EdgectlInstallId = i.scout.installID
	}
	if net.ParseIP(i.address) != nil {
		regData.Ip = i.address
	} else {
		regData.Hostname = i.address
	}
	buf := new(bytes.Buffer)
	_ = json.NewEncoder(buf).Encode(regData)
	resp, err := http.Post(regURL, "application/json", buf)
	if err != nil {
		i.Report("dns_name_failure", ScoutMeta{"err", err.Error()})
		return UnhandledErrResult(errors.Wrap(err, "acquire DNS name (post)"))
	}
	content, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		i.Report("dns_name_failure", ScoutMeta{"err", err.Error()})
		return UnhandledErrResult(errors.Wrap(err, "acquire DNS name (read body)"))
	}

	if resp.StatusCode != 200 {
		message := strings.TrimSpace(string(content))
		i.Report("dns_name_failure", ScoutMeta{"code", resp.StatusCode}, ScoutMeta{"err", message})
		i.show.Println("-> Failed to create a DNS name:", message)

		userMessage := `
<bold>Congratulations! You've successfully installed the Ambassador Edge Stack in your Kubernetes cluster. However, we cannot connect to your cluster from the Internet, so we could not configure TLS automatically.</>

If this IP address is reachable from here, you can access your installation without a DNS name. The following command will open the Edge Policy Console once you accept a self-signed certificate in your browser.
<bold>$ edgectl login -n ambassador {{ .address }}</>

You can use port forwarding to access your Edge Stack installation and the Edge Policy Console.  You will need to accept a self-signed certificate in your browser.
<bold>$ kubectl -n ambassador port-forward deploy/ambassador 8443 &</>
<bold>$ edgectl login -n ambassador 127.0.0.1:8443</>
`
		return Result{
			Message: userMessage,
			URL:     seeDocsURL,
			Report:  "", // FIXME: reported above due to additional metadata required
		}
	}

	i.hostname = string(content)
	i.show.Println("-> Acquiring DNS name", color.Bold.Sprintf(i.hostname))

	// Wait for DNS to propagate. This tries to avoid waiting for a ten
	// minute error backoff if the ACME registration races ahead of the DNS
	// name appearing for LetsEncrypt.
	if err := i.loopUntil("DNS propagation to this host", i.CheckHostnameFound, lc2); err != nil {
		i.Report("dns_name_propagation_timeout")
		i.ShowWrapped("We are unable to resolve your new DNS name on this machine.")
		i.ShowWrapped(seeDocs)
		i.ShowWrapped(tryAgain)
		return UnhandledErrResult(err)
	}
	i.Report("dns_name_propagated")

	// Create a Host resource
	hostResource := fmt.Sprintf(hostManifest, i.hostname, i.hostname, emailAddress)
	if err := i.ShowKubectl("install Host resource", hostResource, "apply", "-f", "-"); err != nil {
		i.Report("fail_host_resource", ScoutMeta{"err", err.Error()})
		i.ShowWrapped("We failed to create a Host resource in your cluster. This is unexpected.")
		i.ShowWrapped(seeDocs)
		return UnhandledErrResult(err)
	}

	i.show.Println("-> Obtaining a TLS certificate from Let's Encrypt")
	if err := i.loopUntil("TLS certificate acquisition", i.CheckACMEIsDone, lc5); err != nil {
		i.Report("cert_provision_failed")
		// Some info is reported by the check function.
		i.ShowWrapped(seeDocs)
		i.ShowWrapped(tryAgain)
		return UnhandledErrResult(err)
	}
	i.Report("cert_provisioned")
	i.show.Println("-> TLS configured successfully")
	if err := i.ShowKubectl("show Host", "", "get", "host", i.hostname); err != nil {
		i.ShowWrapped("We failed to retrieve the Host resource from your cluster that we just created. This is unexpected.")
		i.ShowWrapped(tryAgain)
		return UnhandledErrResult(err)
	}

	i.show.Println()
	i.show.Println("AES Installation Complete!")
	i.show.Println("========================================================================")
	i.show.Println()

	// Show congratulations message
	i.ShowWrapped(color.Bold.Sprintf(fullSuccess, i.hostname))
	i.show.Println()

	// Open a browser window to the Edge Policy Console
	if err := do_login(i.kubeinfo, kcontext, "ambassador", i.hostname, true, true, false); err != nil {
		return UnhandledErrResult(err)
	}

	// Show how to use edgectl login in the future
	i.show.Println()
	i.ShowWrapped(fmt.Sprintf(futureLogin, color.Bold.Sprintf("edgectl login "+i.hostname)))

	if err := i.CheckAESHealth(); err != nil {
		i.Report("aes_health_bad", ScoutMeta{"err", err.Error()})
	} else {
		i.Report("aes_health_good")
	}

	return UnhandledErrResult(nil)
}

// Installer represents the state of the installation process
type Installer struct {
	// Cluster

	kubeinfo    *k8s.KubeInfo
	restConfig  *rest.Config
	coreClient  *k8sClientCoreV1.CoreV1Client
	k8sVersion  *kubernetesVersion
	clusterinfo clusterInfo

	// Reporting

	scout *Scout

	// Logging and management

	ctx     context.Context
	cancel  context.CancelFunc
	show    *log.Logger
	log     *log.Logger
	cmdOut  *log.Logger
	cmdErr  *log.Logger
	logName string

	// Install results

	version   string // which AES is being installed
	address   string // load balancer address
	hostname  string // of the Host resource
	clusterID string // the Ambassador unique clusterID
}

// NewInstaller returns an Installer object after setting up logging.
func NewInstaller(verbose bool) *Installer {
	// Although log, cmdOut, and cmdErr *can* go to different files and/or have
	// different prefixes, they'll probably all go to the same file, possibly
	// with different prefixes, for most cases.
	logfileName := filepath.Join(os.TempDir(), time.Now().Format("edgectl-install-20060102-150405.log"))
	logfile, err := os.Create(logfileName)
	if err != nil {
		logfile = os.Stderr
		fmt.Fprintf(logfile, "WARNING: Failed to open logfile %q: %+v\n", logfileName, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	i := &Installer{
		scout:   NewScout("install"),
		ctx:     ctx,
		cancel:  cancel,
		show:    log.New(io.MultiWriter(os.Stdout, logfile), "", 0),
		logName: logfileName,
	}
	if verbose {
		i.log = log.New(io.MultiWriter(logfile, NewLoggingWriter(log.New(os.Stderr, "== ", 0))), "", log.Ltime)
		i.cmdOut = log.New(io.MultiWriter(logfile, NewLoggingWriter(log.New(os.Stderr, "=- ", 0))), "", 0)
		i.cmdErr = log.New(io.MultiWriter(logfile, NewLoggingWriter(log.New(os.Stderr, "=x ", 0))), "", 0)
	} else {
		i.log = log.New(logfile, "", log.Ltime)
		i.cmdOut = log.New(logfile, "", 0)
		i.cmdErr = log.New(logfile, "", 0)
	}

	return i
}

func (i *Installer) Quit() {
	i.cancel()
}

// ShowWrapped displays to the user (via the show logger) the text items passed
// in with word wrapping applied. Leading and trailing newlines are dropped in
// each text item (to make it easier to use multiline constants), but newlines
// within each item are preserved. Use an empty string item to include a blank
// line in the output between other items.
func (i *Installer) ShowWrapped(texts ...string) {
	for _, text := range texts {
		text = strings.Trim(text, "\n")                  // Drop leading and trailing newlines
		for _, para := range strings.Split(text, "\n") { // Preserve newlines in the text
			for _, line := range doWordWrap(para, "", 79) { // But wrap text too
				i.show.Println(line)
			}
		}
	}
}

// Kubernetes Cluster

// ShowKubectl calls kubectl and dumps the output to the logger. Use this for
// side effects.
func (i *Installer) ShowKubectl(name string, input string, args ...string) error {
	kargs, err := i.kubeinfo.GetKubectlArray(args...)
	if err != nil {
		return errors.Wrapf(err, "cluster access for %s", name)
	}
	kubectl, err := i.GetKubectlPath()
	if err != nil {
		return errors.Wrapf(err, "kubectl not found %s", name)
	}
	i.log.Printf("$ %v %s", kubectl, strings.Join(kargs, " "))
	cmd := exec.Command(kubectl, kargs...)
	cmd.Stdin = strings.NewReader(input)
	cmd.Stdout = NewLoggingWriter(i.cmdOut)
	cmd.Stderr = NewLoggingWriter(i.cmdErr)
	if err := cmd.Run(); err != nil {
		return errors.Wrap(err, name)
	}
	return nil
}

// CaptureKubectl calls kubectl and returns its stdout, dumping all the output
// to the logger.
func (i *Installer) CaptureKubectl(name, input string, args ...string) (res string, err error) {
	res = ""
	kargs, err := i.kubeinfo.GetKubectlArray(args...)
	if err != nil {
		err = errors.Wrapf(err, "cluster access for %s", name)
		return
	}
	kubectl, err := i.GetKubectlPath()
	if err != nil {
		err = errors.Wrapf(err, "kubectl not found %s", name)
		return
	}
	kargs = append([]string{kubectl}, kargs...)
	return i.Capture(name, true, input, kargs...)
}

// SilentCaptureKubectl calls kubectl and returns its stdout
// without dumping all the output to the logger.
func (i *Installer) SilentCaptureKubectl(name, input string, args ...string) (res string, err error) {
	res = ""
	kargs, err := i.kubeinfo.GetKubectlArray(args...)
	if err != nil {
		err = errors.Wrapf(err, "cluster access for %s", name)
		return
	}
	kubectl, err := i.GetKubectlPath()
	if err != nil {
		err = errors.Wrapf(err, "kubectl not found %s", name)
		return
	}
	kargs = append([]string{kubectl}, kargs...)
	return i.Capture(name, false, input, kargs...)
}

// GetCLusterInfo returns the cluster information
func (i *Installer) UpdateClusterInfo() error {
	// Try to determine cluster type from node labels
	if clusterNodeLabels, err := i.CaptureKubectl("get node labels", "", "get", "no", "-Lkubernetes.io/hostname"); err == nil {
		if strings.Contains(clusterNodeLabels, "docker-desktop") {
			i.clusterinfo.name = "docker-desktop"
			i.clusterinfo.isLocal = true
		} else if strings.Contains(clusterNodeLabels, "minikube") {
			i.clusterinfo.name = "minikube"
			i.clusterinfo.isLocal = true
		} else if strings.Contains(clusterNodeLabels, "kind") {
			i.clusterinfo.name = "kind"
			i.clusterinfo.isLocal = true
		} else if strings.Contains(clusterNodeLabels, "k3d") {
			i.clusterinfo.name = "k3d"
			i.clusterinfo.isLocal = true
		} else if strings.Contains(clusterNodeLabels, "gke") {
			i.clusterinfo.name = "gke"
		} else if strings.Contains(clusterNodeLabels, "aks") {
			i.clusterinfo.name = "aks"
		} else if strings.Contains(clusterNodeLabels, "compute") {
			i.clusterinfo.name = "eks"
		} else if strings.Contains(clusterNodeLabels, "ec2") {
			i.clusterinfo.name = "ec2"
		}
	}
	return nil
}

// GetKubectlPath returns the full path to the kubectl executable, or an error if not found
func (i *Installer) GetKubectlPath() (string, error) {
	return exec.LookPath("kubectl")
}

// Capture calls a command and returns its stdout
func (i *Installer) Capture(name string, logToStdout bool, input string, args ...string) (res string, err error) {
	res = ""
	resAsBytes := &bytes.Buffer{}
	i.log.Printf("$ %s", strings.Join(args, " "))
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stdin = strings.NewReader(input)
	if logToStdout {
		cmd.Stdout = io.MultiWriter(NewLoggingWriter(i.cmdOut), resAsBytes)
	} else {
		cmd.Stdout = resAsBytes
	}
	cmd.Stderr = NewLoggingWriter(i.cmdErr)
	err = cmd.Run()
	if err != nil {
		err = errors.Wrap(err, name)
	}
	res = resAsBytes.String()
	return
}

// Metrics

// SetMetadatum adds a key-value pair to the metrics extra traits field. All
// collected metadata is passed with every subsequent report to Metriton.
func (i *Installer) SetMetadatum(name, key string, value interface{}) {
	i.log.Printf("[Metrics] %s (%q) is %q", name, key, value)
	i.scout.SetMetadatum(key, value)
}

// Report sends an event to Metriton
func (i *Installer) Report(eventName string, meta ...ScoutMeta) {
	i.log.Println("[Metrics]", eventName)
	if err := i.scout.Report(eventName, meta...); err != nil {
		i.log.Println("[Metrics]", eventName, err)
	}
}

// clusterInfo describes some properties about the cluster where the installation is performed
type clusterInfo struct {
	name    string
	isLocal bool
}

func doWordWrap(text string, prefix string, lineWidth int) []string {
	words := strings.Fields(strings.TrimSpace(text))
	if len(words) == 0 {
		return []string{""}
	}
	lines := make([]string, 0)
	wrapped := prefix + words[0]
	for _, word := range words[1:] {
		if len(word)+1 > lineWidth-len(wrapped) {
			lines = append(lines, wrapped)
			wrapped = prefix + word
		} else {
			wrapped += " " + word
		}
	}
	if len(wrapped) > 0 {
		lines = append(lines, wrapped)
	}
	return lines
}

type kubernetesVersion struct {
	Client k8sVersion.Info `json:"clientVersion"`
	Server k8sVersion.Info `json:"serverVersion"`
}

// registration is used to register edgestack.me domains
type registration struct {
	Email            string
	Ip               string
	Hostname         string
	EdgectlInstallId string
	AESInstallId     string
}

const hostManifest = `
apiVersion: getambassador.io/v2
kind: Host
metadata:
  name: %s
spec:
  hostname: %s
  acmeProvider:
    email: %s
`

const welcomeInstall = "Installing the Ambassador Edge Stack"

const emailAsk = `Please enter an email address for us to notify you before your TLS certificate and domain name expire. In order to acquire the TLS certificate, we share this email with Let’s Encrypt.`

const beginningAESInstallation = "Beginning Ambassador Edge Stack Installation"

const loginViaIP = "The following command will open the Edge Policy Console once you accept a self-signed certificate in your browser.\n"

const loginViaPortForward = "You can use port forwarding to access your Edge Stack installation and the Edge Policy Console.  You will need to accept a self-signed certificate in your browser.\n"

const failLoadBalancer = `
Timed out waiting for the load balancer's IP address for the AES Service.
- If a load balancer IP address shows up, simply run the installer again.
- If your cluster doesn't support load balancers, you'll need to expose AES some other way.
`

const tryAgain = "If this appears to be a transient failure, please try running the installer again. It is safe to run the installer repeatedly on a cluster."

const abortExisting = `
This tool does not support upgrades/downgrades at this time.
The installer will now quit to avoid corrupting an existing installation of AES.
`

const seeDocsURL = "https://www.getambassador.io/docs/latest/tutorials/getting-started/"
const seeDocs = "See " + seeDocsURL

const phoneHomeDisabled = "INFO: phone-home is disabled by environment variable"
const installAndTraceIDs = "INFO: install_id = %s; trace_id = %s"

var validEmailAddress = regexp.MustCompile("^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$")

const fullSuccess = `Congratulations! You've successfully installed the Ambassador Edge Stack in your Kubernetes cluster. Visit https://%s` // hostname

const futureLogin = `In the future, to log in to the Ambassador Edge Policy Console, run
$ %s` // "edgectl login <hostname>"

const noTlsSuccess = "Congratulations! You've successfully installed the Ambassador Edge Stack in your Kubernetes cluster. However, we cannot connect to your cluster from the Internet, so we could not configure TLS automatically."

const noKubectlURL = "https://kubernetes.io/docs/tasks/tools/install-kubectl/"
const noKubectl = `
The installer depends on the 'kubectl' executable. Make sure you have the latest release downloaded in your PATH, and that you have executable permissions.
Visit ` + noKubectlURL + ` for more information and instructions.`

const noClusterURL = "https://kubernetes.io/docs/setup/"
const noCluster = `
Unable to communicate with the remote Kubernetes cluster using your kubectl context.
To further debug and diagnose cluster problems, use 'kubectl cluster-info dump'
or get started and run Kubernetes: ` + noClusterURL
