package nodelifecycle

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/golang/glog"

	coordv1 "k8s.io/api/coordination/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	coordinformers "k8s.io/client-go/informers/coordination/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	coordlisters "k8s.io/client-go/listers/coordination/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	kube_rest "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	kubeClientCmd "k8s.io/client-go/tools/clientcmd"
	kubeClientCmdApi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/homedir"
	"k8s.io/klog"
	"k8s.io/node-problem-detector/cmd/options"
	"k8s.io/node-problem-detector/pkg/exporters/k8sexporter/problemclient"
)

const (
	APIVersion = "v1"

	// default re-sync period for all informer factories
	defaultResync          = 600 * time.Second
	defaultInClusterConfig = true

	nodeNameKeyIndex = "spec.nodeName"
	masterLabelKey   = "node-role.kubernetes.io/master"

	// The amount of time the nodecontroller should sleep between retrying node health updates
	retrySleepTime = 20 * time.Millisecond
	// NodeHealthUpdateRetry controls the number of retries of writing
	// node health update.
	NodeHealthUpdateRetry = 5
	// TODO: configurable
	nodeMonitorPeriod      = 5 * time.Second
	nodeMonitorGracePeriod = 10 * time.Second
)

var (
	// UnreachableTaintTemplate is the taint for when a node becomes unreachable.
	UnreachableTaintTemplate = &v1.Taint{
		Key:    v1.TaintNodeUnreachable,
		Effect: v1.TaintEffectNoExecute,
	}

	// NotReadyTaintTemplate is the taint for when a node is not ready for
	// executing pods
	NotReadyTaintTemplate = &v1.Taint{
		Key:    v1.TaintNodeNotReady,
		Effect: v1.TaintEffectNoExecute,
	}

	// map {NodeConditionType: {ConditionStatus: TaintKey}}
	// represents which NodeConditionType under which ConditionStatus should be
	// tainted with which TaintKey
	// for certain NodeConditionType, there are multiple {ConditionStatus,TaintKey} pairs
	nodeConditionToTaintKeyStatusMap = map[v1.NodeConditionType]map[v1.ConditionStatus]string{
		v1.NodeReady: {
			v1.ConditionFalse:   v1.TaintNodeNotReady,
			v1.ConditionUnknown: v1.TaintNodeUnreachable,
		},
		v1.NodeMemoryPressure: {
			v1.ConditionTrue: v1.TaintNodeMemoryPressure,
		},
		v1.NodeDiskPressure: {
			v1.ConditionTrue: v1.TaintNodeDiskPressure,
		},
		v1.NodeNetworkUnavailable: {
			v1.ConditionTrue: v1.TaintNodeNetworkUnavailable,
		},
		v1.NodePIDPressure: {
			v1.ConditionTrue: v1.TaintNodePIDPressure,
		},
	}

	taintKeyToNodeConditionMap = map[string]v1.NodeConditionType{
		v1.TaintNodeNotReady:           v1.NodeReady,
		v1.TaintNodeUnreachable:        v1.NodeReady,
		v1.TaintNodeNetworkUnavailable: v1.NodeNetworkUnavailable,
		v1.TaintNodeMemoryPressure:     v1.NodeMemoryPressure,
		v1.TaintNodeDiskPressure:       v1.NodeDiskPressure,
		v1.TaintNodePIDPressure:        v1.NodePIDPressure,
	}
)

// Controller is the controller that manages node's life cycle.
type Controller struct {
	podLister         corelisters.PodLister
	podInformerSynced cache.InformerSynced
	kubeClient        clientset.Interface

	// This timestamp is to be used instead of LastProbeTime stored in Condition. We do this
	// to avoid the problem with time skew across the cluster.
	now func() metav1.Time

	leaseLister         coordlisters.LeaseLister
	leaseInformerSynced cache.InformerSynced
	nodeLister          corelisters.NodeLister
	nodeInformerSynced  cache.InformerSynced

	getPodsAssignedToNode func(nodeName string) ([]*v1.Pod, error)

	recorder     record.EventRecorder
	nodesToRetry sync.Map
	// per Node map storing last observed health together with a local time when it was observed.
	nodeHealthMap *nodeHealthMap

	// Value controlling Controller monitoring period, i.e. how often does Controller
	// check node health signal posted from kubelet. This value should be lower than
	// nodeMonitorGracePeriod.
	// TODO: Change node health monitor to watch based.
	nodeMonitorPeriod time.Duration

	// When node is just created, e.g. cluster bootstrap or node creation, we give
	// a longer grace period.
	nodeStartupGracePeriod time.Duration

	// Controller will not proactively sync node health, but will monitor node
	// health signal updated from kubelet. There are 2 kinds of node healthiness
	// signals: NodeStatus and NodeLease. If it doesn't receive update for this amount
	// of time, it will start posting "NodeReady==ConditionUnknown". The amount of
	// time before which Controller start evicting pods is controlled via flag
	// 'pod-eviction-timeout'.
	// Note: be cautious when changing the constant, it must work with
	// nodeStatusUpdateFrequency in kubelet and renewInterval in NodeLease
	// controller. The node health signal update frequency is the minimal of the
	// two.
	// There are several constraints:
	// 1. nodeMonitorGracePeriod must be N times more than  the node health signal
	//    update frequency, where N means number of retries allowed for kubelet to
	//    post node status/lease. It is pointless to make nodeMonitorGracePeriod
	//    be less than the node health signal update frequency, since there will
	//    only be fresh values from Kubelet at an interval of node health signal
	//    update frequency. The constant must be less than podEvictionTimeout.
	// 2. nodeMonitorGracePeriod can't be too large for user experience - larger
	//    value takes longer for user to see up-to-date node health.
	nodeMonitorGracePeriod time.Duration

	podEvictionTimeout time.Duration

	// if set to true Controller will start TaintManager that will evict Pods from
	// tainted nodes, if they're not tolerated.
	runTaintManager bool
}

type nodeHealthData struct {
	probeTimestamp           metav1.Time
	readyTransitionTimestamp metav1.Time
	status                   *v1.NodeStatus
	lease                    *coordv1.Lease
}

func (n *nodeHealthData) deepCopy() *nodeHealthData {
	if n == nil {
		return nil
	}
	return &nodeHealthData{
		probeTimestamp:           n.probeTimestamp,
		readyTransitionTimestamp: n.readyTransitionTimestamp,
		status:                   n.status.DeepCopy(),
		lease:                    n.lease.DeepCopy(),
	}
}

type nodeHealthMap struct {
	lock        sync.RWMutex
	nodeHealths map[string]*nodeHealthData
}

func newNodeHealthMap() *nodeHealthMap {
	return &nodeHealthMap{
		nodeHealths: make(map[string]*nodeHealthData),
	}
}

// getDeepCopy - returns copy of node health data.
// It prevents data being changed after retrieving it from the map.
func (n *nodeHealthMap) getDeepCopy(name string) *nodeHealthData {
	n.lock.RLock()
	defer n.lock.RUnlock()
	return n.nodeHealths[name].deepCopy()
}

func (n *nodeHealthMap) set(name string, data *nodeHealthData) {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.nodeHealths[name] = data
}

func NewNodeMonitorOrDie(ctx context.Context, npdo *options.NodeProblemDetectorOptions) {
	c := problemclient.NewClientOrDie(npdo)
	node, err := c.GetNode(ctx)
	if err != nil {
		glog.Errorf("Can't get node object: %v", err)
		return
	}
	// This node monitor is just running on the master.
	if _, ok := node.Labels[masterLabelKey]; !ok {
		return
	}

	// we have checked it is a valid URI after command line argument is parsed.:)
	uri, _ := url.Parse(npdo.ApiServerOverride)
	cfg, err := getKubeClientConfig(uri)
	if err != nil {
		glog.Fatalf("Failed to get client config: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		glog.Fatalf("Failed to create kubernetes client: %v", err)
	}

	sharedInformers := informers.NewSharedInformerFactory(clientset, defaultResync)
	lifecycleController, err := NewNodeLifecycleController(
		sharedInformers.Coordination().V1().Leases(),
		sharedInformers.Core().V1().Pods(),
		sharedInformers.Core().V1().Nodes(),
		clientset,
		nodeMonitorPeriod,
		nodeMonitorGracePeriod,
	)
	if err != nil {
		glog.Errorf("Failed to get lifecycle controller: %v", err)
		return
	}
	klog.Infof("Starting node controller")
	go sharedInformers.Start(wait.NeverStop)
	go lifecycleController.Run(ctx)
}

// NewNodeLifecycleController returns a new taint controller.
func NewNodeLifecycleController(
	leaseInformer coordinformers.LeaseInformer,
	podInformer coreinformers.PodInformer,
	nodeInformer coreinformers.NodeInformer,
	kubeClient clientset.Interface,
	nodeMonitorPeriod time.Duration,
	nodeMonitorGracePeriod time.Duration,
) (*Controller, error) {
	eventBroadcaster := record.NewBroadcaster()
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "node-controller"})
	nc := &Controller{
		kubeClient:             kubeClient,
		now:                    metav1.Now,
		recorder:               recorder,
		nodesToRetry:           sync.Map{},
		nodeMonitorPeriod:      nodeMonitorPeriod,
		nodeMonitorGracePeriod: nodeMonitorGracePeriod,
		nodeStartupGracePeriod: nodeMonitorGracePeriod,
	}

	nc.podInformerSynced = podInformer.Informer().HasSynced
	podInformer.Informer().AddIndexers(cache.Indexers{
		nodeNameKeyIndex: func(obj interface{}) ([]string, error) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				return []string{}, nil
			}
			if len(pod.Spec.NodeName) == 0 {
				return []string{}, nil
			}
			return []string{pod.Spec.NodeName}, nil
		},
	})

	podIndexer := podInformer.Informer().GetIndexer()
	nc.getPodsAssignedToNode = func(nodeName string) ([]*v1.Pod, error) {
		objs, err := podIndexer.ByIndex(nodeNameKeyIndex, nodeName)
		if err != nil {
			return nil, err
		}
		pods := make([]*v1.Pod, 0, len(objs))
		for _, obj := range objs {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				continue
			}
			pods = append(pods, pod)
		}
		return pods, nil
	}
	nc.podLister = podInformer.Lister()
	nc.nodeLister = nodeInformer.Lister()

	nc.leaseLister = leaseInformer.Lister()
	nc.leaseInformerSynced = leaseInformer.Informer().HasSynced

	nc.nodeInformerSynced = nodeInformer.Informer().HasSynced

	return nc, nil
}

// Run starts an asynchronous loop that monitors the status of cluster nodes.
func (nc *Controller) Run(ctx context.Context) {
	defer utilruntime.HandleCrash()

	defer klog.Infof("Shutting down node controller")
	if !cache.WaitForNamedCacheSync("taint", ctx.Done(), nc.leaseInformerSynced, nc.nodeInformerSynced, nc.podInformerSynced) {
		return
	}

	// Incorporate the results of node health signal pushed from kubelet to master.
	go wait.UntilWithContext(ctx, func(ctx context.Context) {
		if err := nc.monitorNodeHealth(ctx); err != nil {
			klog.Errorf("Error monitoring node health: %v", err)
		}
	}, nc.nodeMonitorPeriod)

	<-ctx.Done()
}

// monitorNodeHealth verifies node health are constantly updated by kubelet, and
// if not, post "NodeReady==ConditionUnknown".
// This function will taint nodes who are not ready or not reachable for a long period of time.
func (nc *Controller) monitorNodeHealth(ctx context.Context) error {
	// We are listing nodes from local cache as we can tolerate some small delays
	// comparing to state from etcd and there is eventual consistency anyway.
	nodes, err := nc.nodeLister.List(labels.Everything())
	if err != nil {
		return err
	}

	for i := range nodes {
		var gracePeriod time.Duration
		var observedReadyCondition v1.NodeCondition
		var currentReadyCondition *v1.NodeCondition
		node := nodes[i].DeepCopy()
		if err := wait.PollImmediate(retrySleepTime, retrySleepTime*NodeHealthUpdateRetry, func() (bool, error) {
			gracePeriod, observedReadyCondition, currentReadyCondition, err = nc.tryUpdateNodeHealth(ctx, node)
			if err == nil {
				return true, nil
			}
			name := node.Name
			node, err = nc.kubeClient.CoreV1().Nodes().Get(name, metav1.GetOptions{})
			if err != nil {
				klog.Errorf("Failed while getting a Node to retry updating node health. Probably Node %s was deleted.", name)
				return false, err
			}
			return false, nil
		}); err != nil {
			klog.Errorf("Update health of Node '%v' from Controller error: %v. "+
				"Skipping - no pods will be evicted.", node.Name, err)
			continue
		}

		if currentReadyCondition != nil {
			pods, err := nc.getPodsAssignedToNode(node.Name)
			if err != nil {
				utilruntime.HandleError(fmt.Errorf("unable to list pods of node %v: %v", node.Name, err))
				if currentReadyCondition.Status != v1.ConditionTrue && observedReadyCondition.Status == v1.ConditionTrue {
					// If error happened during node status transition (Ready -> NotReady)
					// we need to mark node for retry to force MarkPodsNotReady execution
					// in the next iteration.
					nc.nodesToRetry.Store(node.Name, struct{}{})
				}
				continue
			}
			if nc.runTaintManager {
				nc.processTaintBaseEviction(ctx, node, &observedReadyCondition)
			} else {
				if err := nc.processNoTaintBaseEviction(ctx, node, &observedReadyCondition, gracePeriod, pods); err != nil {
					utilruntime.HandleError(fmt.Errorf("unable to evict all pods from node %v: %v; queuing for retry", node.Name, err))
				}
			}

			_, needsRetry := nc.nodesToRetry.Load(node.Name)
			switch {
			case currentReadyCondition.Status != v1.ConditionTrue && observedReadyCondition.Status == v1.ConditionTrue:
				// Report node event only once when status changed.
				RecordNodeStatusChange(nc.recorder, node, "NodeNotReady")
				fallthrough
			case needsRetry && observedReadyCondition.Status != v1.ConditionTrue:
				if err = MarkPodsNotReady(ctx, nc.kubeClient, nc.recorder, pods, node.Name); err != nil {
					utilruntime.HandleError(fmt.Errorf("unable to mark all pods NotReady on node %v: %v; queuing for retry", node.Name, err))
					nc.nodesToRetry.Store(node.Name, struct{}{})
					continue
				}
			}
		}
		nc.nodesToRetry.Delete(node.Name)
	}

	return nil
}

func (nc *Controller) processTaintBaseEviction(ctx context.Context, node *v1.Node, observedReadyCondition *v1.NodeCondition) {
	decisionTimestamp := nc.now()
	// Check eviction timeout against decisionTimestamp
	switch observedReadyCondition.Status {
	case v1.ConditionFalse:
		// We want to update the taint straight away if Node is already tainted with the UnreachableTaint
		if TaintExists(node.Spec.Taints, UnreachableTaintTemplate) {
			taintToAdd := *NotReadyTaintTemplate
			if !SwapNodeControllerTaint(ctx, nc.kubeClient, []*v1.Taint{&taintToAdd}, []*v1.Taint{UnreachableTaintTemplate}, node) {
				klog.Errorf("Failed to instantly swap UnreachableTaint to NotReadyTaint. Will try again in the next cycle.")
			}
		} else if nc.markNodeForTainting(ctx, node, v1.ConditionFalse) {
			klog.V(2).Infof("Node %v is NotReady as of %v. Adding it to the Taint queue.",
				node.Name,
				decisionTimestamp,
			)
		}
	case v1.ConditionUnknown:
		// We want to update the taint straight away if Node is already tainted with the UnreachableTaint
		if TaintExists(node.Spec.Taints, NotReadyTaintTemplate) {
			taintToAdd := *UnreachableTaintTemplate
			if !SwapNodeControllerTaint(ctx, nc.kubeClient, []*v1.Taint{&taintToAdd}, []*v1.Taint{NotReadyTaintTemplate}, node) {
				klog.Errorf("Failed to instantly swap NotReadyTaint to UnreachableTaint. Will try again in the next cycle.")
			}
		} else if nc.markNodeForTainting(ctx, node, v1.ConditionUnknown) {
			klog.V(2).Infof("Node %v is unresponsive as of %v. Adding it to the Taint queue.",
				node.Name,
				decisionTimestamp,
			)
		}
	case v1.ConditionTrue:
		removed, err := nc.markNodeAsReachable(ctx, node)
		if err != nil {
			klog.Errorf("Failed to remove taints from node %v. Will retry in next iteration.", node.Name)
		}
		if removed {
			klog.V(2).Infof("Node %s is healthy again, removing all taints", node.Name)
		}
	}
}

func (nc *Controller) processNoTaintBaseEviction(ctx context.Context, node *v1.Node, observedReadyCondition *v1.NodeCondition, gracePeriod time.Duration, pods []*v1.Pod) error {
	decisionTimestamp := nc.now()
	nodeHealthData := nc.nodeHealthMap.getDeepCopy(node.Name)
	if nodeHealthData == nil {
		return fmt.Errorf("health data doesn't exist for node %q", node.Name)
	}
	// Check eviction timeout against decisionTimestamp
	switch observedReadyCondition.Status {
	case v1.ConditionFalse:
		if decisionTimestamp.After(nodeHealthData.readyTransitionTimestamp.Add(nc.podEvictionTimeout)) {
			enqueued, err := nc.evictPods(ctx, node, pods)
			if err != nil {
				return err
			}
			if enqueued {
				klog.V(2).Infof("Node is NotReady. Adding Pods on Node %s to eviction queue: %v is later than %v + %v",
					node.Name,
					decisionTimestamp,
					nodeHealthData.readyTransitionTimestamp,
					nc.podEvictionTimeout,
				)
			}
		}
	case v1.ConditionUnknown:
		if decisionTimestamp.After(nodeHealthData.probeTimestamp.Add(nc.podEvictionTimeout)) {
			enqueued, err := nc.evictPods(ctx, node, pods)
			if err != nil {
				return err
			}
			if enqueued {
				klog.V(2).Infof("Node is unresponsive. Adding Pods on Node %s to eviction queues: %v is later than %v + %v",
					node.Name,
					decisionTimestamp,
					nodeHealthData.readyTransitionTimestamp,
					nc.podEvictionTimeout-gracePeriod,
				)
			}
		}
	case v1.ConditionTrue:
		// TODO: scheduler handle
	}
	return nil
}

// tryUpdateNodeHealth checks a given node's conditions and tries to update it. Returns grace period to
// which given node is entitled, state of current and last observed Ready Condition, and an error if it occurred.
func (nc *Controller) tryUpdateNodeHealth(ctx context.Context, node *v1.Node) (time.Duration, v1.NodeCondition, *v1.NodeCondition, error) {
	nodeHealth := nc.nodeHealthMap.getDeepCopy(node.Name)
	defer func() {
		nc.nodeHealthMap.set(node.Name, nodeHealth)
	}()

	var gracePeriod time.Duration
	var observedReadyCondition v1.NodeCondition
	_, currentReadyCondition := GetNodeCondition(&node.Status, v1.NodeReady)
	if currentReadyCondition == nil {
		// If ready condition is nil, then kubelet (or nodecontroller) never posted node status.
		// A fake ready condition is created, where LastHeartbeatTime and LastTransitionTime is set
		// to node.CreationTimestamp to avoid handle the corner case.
		observedReadyCondition = v1.NodeCondition{
			Type:               v1.NodeReady,
			Status:             v1.ConditionUnknown,
			LastHeartbeatTime:  node.CreationTimestamp,
			LastTransitionTime: node.CreationTimestamp,
		}
		gracePeriod = nc.nodeStartupGracePeriod
		if nodeHealth != nil {
			nodeHealth.status = &node.Status
		} else {
			nodeHealth = &nodeHealthData{
				status:                   &node.Status,
				probeTimestamp:           node.CreationTimestamp,
				readyTransitionTimestamp: node.CreationTimestamp,
			}
		}
	} else {
		// If ready condition is not nil, make a copy of it, since we may modify it in place later.
		observedReadyCondition = *currentReadyCondition
		gracePeriod = nc.nodeMonitorGracePeriod
	}
	// There are following cases to check:
	// - both saved and new status have no Ready Condition set - we leave everything as it is,
	// - saved status have no Ready Condition, but current one does - Controller was restarted with Node data already present in etcd,
	// - saved status have some Ready Condition, but current one does not - it's an error, but we fill it up because that's probably a good thing to do,
	// - both saved and current statuses have Ready Conditions and they have the same LastProbeTime - nothing happened on that Node, it may be
	//   unresponsive, so we leave it as it is,
	// - both saved and current statuses have Ready Conditions, they have different LastProbeTimes, but the same Ready Condition State -
	//   everything's in order, no transition occurred, we update only probeTimestamp,
	// - both saved and current statuses have Ready Conditions, different LastProbeTimes and different Ready Condition State -
	//   Ready Condition changed it state since we last seen it, so we update both probeTimestamp and readyTransitionTimestamp.
	// TODO: things to consider:
	//   - if 'LastProbeTime' have gone back in time its probably an error, currently we ignore it,
	//   - currently only correct Ready State transition outside of Node Controller is marking it ready by Kubelet, we don't check
	//     if that's the case, but it does not seem necessary.
	var savedCondition *v1.NodeCondition
	var savedLease *coordv1.Lease
	if nodeHealth != nil {
		_, savedCondition = GetNodeCondition(nodeHealth.status, v1.NodeReady)
		savedLease = nodeHealth.lease
	}

	if nodeHealth == nil {
		klog.Warningf("Missing timestamp for Node %s. Assuming now as a timestamp.", node.Name)
		nodeHealth = &nodeHealthData{
			status:                   &node.Status,
			probeTimestamp:           nc.now(),
			readyTransitionTimestamp: nc.now(),
		}
	} else if savedCondition == nil && currentReadyCondition != nil {
		klog.V(1).Infof("Creating timestamp entry for newly observed Node %s", node.Name)
		nodeHealth = &nodeHealthData{
			status:                   &node.Status,
			probeTimestamp:           nc.now(),
			readyTransitionTimestamp: nc.now(),
		}
	} else if savedCondition != nil && currentReadyCondition == nil {
		klog.Errorf("ReadyCondition was removed from Status of Node %s", node.Name)
		// TODO: figure out what to do in this case. For now we do the same thing as above.
		nodeHealth = &nodeHealthData{
			status:                   &node.Status,
			probeTimestamp:           nc.now(),
			readyTransitionTimestamp: nc.now(),
		}
	} else if savedCondition != nil && currentReadyCondition != nil && savedCondition.LastHeartbeatTime != currentReadyCondition.LastHeartbeatTime {
		var transitionTime metav1.Time
		// If ReadyCondition changed since the last time we checked, we update the transition timestamp to "now",
		// otherwise we leave it as it is.
		if savedCondition.LastTransitionTime != currentReadyCondition.LastTransitionTime {
			klog.V(3).Infof("ReadyCondition for Node %s transitioned from %v to %v", node.Name, savedCondition, currentReadyCondition)
			transitionTime = nc.now()
		} else {
			transitionTime = nodeHealth.readyTransitionTimestamp
		}
		klog.V(3).Infof("Node %s ReadyCondition updated. Updating timestamp.", node.Name)
		nodeHealth = &nodeHealthData{
			status:                   &node.Status,
			probeTimestamp:           nc.now(),
			readyTransitionTimestamp: transitionTime,
		}
	}
	// Always update the probe time if node lease is renewed.
	// Note: If kubelet never posted the node status, but continues renewing the
	// heartbeat leases, the node controller will assume the node is healthy and
	// take no action.
	observedLease, _ := nc.leaseLister.Leases(v1.NamespaceNodeLease).Get(node.Name)
	if observedLease != nil && (savedLease == nil || savedLease.Spec.RenewTime.Before(observedLease.Spec.RenewTime)) {
		nodeHealth.lease = observedLease
		nodeHealth.probeTimestamp = nc.now()
	}

	if nc.now().After(nodeHealth.probeTimestamp.Add(gracePeriod)) {
		// NodeReady condition or lease was last set longer ago than gracePeriod, so
		// update it to Unknown (regardless of its current value) in the master.

		nodeConditionTypes := []v1.NodeConditionType{
			v1.NodeReady,
			v1.NodeMemoryPressure,
			v1.NodeDiskPressure,
			v1.NodePIDPressure,
			// We don't change 'NodeNetworkUnavailable' condition, as it's managed on a control plane level.
			// v1.NodeNetworkUnavailable,
		}

		nowTimestamp := nc.now()
		for _, nodeConditionType := range nodeConditionTypes {
			_, currentCondition := GetNodeCondition(&node.Status, nodeConditionType)
			if currentCondition == nil {
				klog.V(2).Infof("Condition %v of node %v was never updated by kubelet", nodeConditionType, node.Name)
				node.Status.Conditions = append(node.Status.Conditions, v1.NodeCondition{
					Type:               nodeConditionType,
					Status:             v1.ConditionUnknown,
					Reason:             "NodeStatusNeverUpdated",
					Message:            "Kubelet never posted node status.",
					LastHeartbeatTime:  node.CreationTimestamp,
					LastTransitionTime: nowTimestamp,
				})
			} else {
				klog.V(2).Infof("node %v hasn't been updated for %+v. Last %v is: %+v",
					node.Name, nc.now().Time.Sub(nodeHealth.probeTimestamp.Time), nodeConditionType, currentCondition)
				if currentCondition.Status != v1.ConditionUnknown {
					currentCondition.Status = v1.ConditionUnknown
					currentCondition.Reason = "NodeStatusUnknown"
					currentCondition.Message = "Kubelet stopped posting node status."
					currentCondition.LastTransitionTime = nowTimestamp
				}
			}
		}
		// We need to update currentReadyCondition due to its value potentially changed.
		_, currentReadyCondition = GetNodeCondition(&node.Status, v1.NodeReady)

		if !reflect.DeepEqual(currentReadyCondition, &observedReadyCondition) {
			if _, err := nc.kubeClient.CoreV1().Nodes().UpdateStatus(node); err != nil {
				klog.Errorf("Error updating node %s: %v", node.Name, err)
				return gracePeriod, observedReadyCondition, currentReadyCondition, err
			}
			nodeHealth = &nodeHealthData{
				status:                   &node.Status,
				probeTimestamp:           nodeHealth.probeTimestamp,
				readyTransitionTimestamp: nc.now(),
				lease:                    observedLease,
			}
			return gracePeriod, observedReadyCondition, currentReadyCondition, nil
		}
	}

	return gracePeriod, observedReadyCondition, currentReadyCondition, nil
}

func (nc *Controller) doNoExecuteTaintingPass(ctx context.Context, node *v1.Node) bool {
	_, condition := GetNodeCondition(&node.Status, v1.NodeReady)
	// Because we want to mimic NodeStatus.Condition["Ready"] we make "unreachable" and "not ready" taints mutually exclusive.
	taintToAdd := v1.Taint{}
	oppositeTaint := v1.Taint{}
	switch condition.Status {
	case v1.ConditionFalse:
		taintToAdd = *NotReadyTaintTemplate
		oppositeTaint = *UnreachableTaintTemplate
	case v1.ConditionUnknown:
		taintToAdd = *UnreachableTaintTemplate
		oppositeTaint = *NotReadyTaintTemplate
	default:
		// It seems that the Node is ready again, so there's no need to taint it.
		klog.V(4).Infof("Node %v was in a taint queue, but it's ready now. Ignoring taint request.", node.Name)
		return true
	}

	return SwapNodeControllerTaint(ctx, nc.kubeClient, []*v1.Taint{&taintToAdd}, []*v1.Taint{&oppositeTaint}, node)
}

func (nc *Controller) doNoScheduleTaintingPass(ctx context.Context, node *v1.Node) error {
	// Map node's condition to Taints.
	var taints []v1.Taint
	for _, condition := range node.Status.Conditions {
		if taintMap, found := nodeConditionToTaintKeyStatusMap[condition.Type]; found {
			if taintKey, found := taintMap[condition.Status]; found {
				taints = append(taints, v1.Taint{
					Key:    taintKey,
					Effect: v1.TaintEffectNoSchedule,
				})
			}
		}
	}
	if node.Spec.Unschedulable {
		// If unschedulable, append related taint.
		taints = append(taints, v1.Taint{
			Key:    v1.TaintNodeUnschedulable,
			Effect: v1.TaintEffectNoSchedule,
		})
	}

	// Get exist taints of node.
	nodeTaints := TaintSetFilter(node.Spec.Taints, func(t *v1.Taint) bool {
		// only NoSchedule taints are candidates to be compared with "taints" later
		if t.Effect != v1.TaintEffectNoSchedule {
			return false
		}
		// Find unschedulable taint of node.
		if t.Key == v1.TaintNodeUnschedulable {
			return true
		}
		// Find node condition taints of node.
		_, found := taintKeyToNodeConditionMap[t.Key]
		return found
	})
	taintsToAdd, taintsToDel := TaintSetDiff(taints, nodeTaints)
	// If nothing to add or delete, return true directly.
	if len(taintsToAdd) == 0 && len(taintsToDel) == 0 {
		return nil
	}
	if !SwapNodeControllerTaint(ctx, nc.kubeClient, taintsToAdd, taintsToDel, node) {
		return fmt.Errorf("failed to swap taints of node %+v", node)
	}

	return nil
}

// evictPods:
//   - adds node to evictor queue if the node is not marked as evicted.
//     Returns false if the node name was already enqueued.
//   - deletes pods immediately if node is already marked as evicted.
//     Returns false, because the node wasn't added to the queue.
func (nc *Controller) evictPods(ctx context.Context, node *v1.Node, pods []*v1.Pod) (bool, error) {
	// Node eviction already happened for this node.
	// Handling immediate pod deletion.
	// TODO: delete endpoints
	//_, err := DeletePods(ctx, nc.kubeClient, pods, nc.recorder, node.Name, string(node.UID), nc.daemonSetStore)
	//if err != nil {
	//	return false, fmt.Errorf("unable to delete pods from node %q: %v", node.Name, err)
	//}
	return false, nil
}

func (nc *Controller) markNodeForTainting(ctx context.Context, node *v1.Node, status v1.ConditionStatus) bool {
	if status == v1.ConditionFalse {
		if !TaintExists(node.Spec.Taints, NotReadyTaintTemplate) {
			return true
		}
	}

	if status == v1.ConditionUnknown {
		if !TaintExists(node.Spec.Taints, UnreachableTaintTemplate) {
			return true
		}
	}

	if err := nc.doNoScheduleTaintingPass(ctx, node); err != nil {
		return false
	}

	return nc.doNoExecuteTaintingPass(ctx, node)
}

func (nc *Controller) markNodeAsReachable(ctx context.Context, node *v1.Node) (bool, error) {
	err := RemoveTaintOffNode(ctx, nc.kubeClient, node.Name, node, UnreachableTaintTemplate)
	if err != nil {
		klog.Errorf("Failed to remove taint from node %v: %v", node.Name, err)
		return false, err
	}
	err = RemoveTaintOffNode(ctx, nc.kubeClient, node.Name, node, NotReadyTaintTemplate)
	if err != nil {
		klog.Errorf("Failed to remove taint from node %v: %v", node.Name, err)
		return false, err
	}

	return true, nil
}

func getKubeClientConfig(uri *url.URL) (*kube_rest.Config, error) {
	var (
		kubeConfig *kube_rest.Config
		err        error
	)

	opts := uri.Query()
	configOverrides, err := getConfigOverrides(uri)
	if err != nil {
		return nil, err
	}

	inClusterConfig := defaultInClusterConfig
	if len(opts["inClusterConfig"]) > 0 {
		inClusterConfig, err = strconv.ParseBool(opts["inClusterConfig"][0])
		if err != nil {
			return nil, err
		}
	}

	if inClusterConfig {
		kubeConfig, err = kube_rest.InClusterConfig()
		if err != nil {
			return nil, err
		}

		if configOverrides.ClusterInfo.Server != "" {
			kubeConfig.Host = configOverrides.ClusterInfo.Server
		}
		kubeConfig.GroupVersion = &schema.GroupVersion{Version: APIVersion}
		kubeConfig.Insecure = configOverrides.ClusterInfo.InsecureSkipTLSVerify
		if configOverrides.ClusterInfo.InsecureSkipTLSVerify {
			kubeConfig.TLSClientConfig.CAFile = ""
		}
	} else {
		var kubeconfig *string
		if home := homedir.HomeDir(); home != "" {
			kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
		} else {
			kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
		}
		flag.Parse()

		kubeConfig, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
		if err != nil {
			return nil, err
		}
	}

	return kubeConfig, nil
}

func getConfigOverrides(uri *url.URL) (*kubeClientCmd.ConfigOverrides, error) {
	kubeConfigOverride := kubeClientCmd.ConfigOverrides{
		ClusterInfo: kubeClientCmdApi.Cluster{},
	}
	if len(uri.Scheme) != 0 && len(uri.Host) != 0 {
		kubeConfigOverride.ClusterInfo.Server = fmt.Sprintf("%s://%s", uri.Scheme, uri.Host)
	}

	opts := uri.Query()

	if len(opts["insecure"]) > 0 {
		insecure, err := strconv.ParseBool(opts["insecure"][0])
		if err != nil {
			return nil, err
		}
		kubeConfigOverride.ClusterInfo.InsecureSkipTLSVerify = insecure
	}

	return &kubeConfigOverride, nil
}
