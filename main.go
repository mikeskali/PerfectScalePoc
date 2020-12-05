package main

import (
	"encoding/csv"
	"fmt"
	"crypto/md5"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/mikeskali/PerfectScalePoc/clustercache"
	"github.com/mikeskali/PerfectScalePoc/env"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var ignoreValues = []string{"kubernetes.io/hostname","topology.kubernetes.io/zone", "failure-domain.beta.kubernetes.io/zone","logzio/az"}
var shouldHash bool


func main() {
	fmt.Println("Let's optimize stuff")
	kubCfgPath := env.Get("KUBECONFIG_PATH","")
	if kubCfgPath == "" {
		fmt.Println("KUBECONFIG_PATH not set, exiting")
		return
	}

	shouldHash = env.GetBool("SHOULD_HASH",false)
	
	var err error

	var kc *rest.Config
	// init kubernetes API setup
	
	//If have kubecfg => use it, otherwise, inClusterConfig
	if kubeconfig := env.GetKubeConfigPath(); kubeconfig != "" {
		kc, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		kc, err = rest.InClusterConfig()
	}

	if err != nil {
		panic(err.Error())
	}

	kubeClientset, err := kubernetes.NewForConfig(kc)
	if err != nil {
		log.Fatal(err.Error())
	}

	// Create Kubernetes Cluster Cache + Watchers
	k8sCache := clustercache.NewKubernetesClusterCache(kubeClientset)
	k8sCache.Run()

	wd,err := os.Getwd()
	if err != nil {
		log.Fatal("Can't open working dir")
	} else {
		log.Printf("\nworking dir: %s\n\n", wd)
	}
	
	
	nodes2groups := printNodeGroups(k8sCache)
	fmt.Println()
	fmt.Println()

	printDaemonSets(k8sCache)
	fmt.Println()
	fmt.Println()

	printStatefulSets(k8sCache)
	fmt.Println()
	fmt.Println()

	printDeployments(k8sCache)

	printPods(k8sCache, nodes2groups)
}

func printPods(k8sCache clustercache.ClusterCache, node2group map[string]string){
	podsCsv, err := os.Create("pods.csv")
	defer podsCsv.Close()
	podsRecords := [][]string{
        {"pod_name","node_name","node_group","namespace","owner_kind","owner_name","req_cpu_milli_core", "req_mem_byte","limit_cpu_mili_core","limit_mem_bytes"},
	}

	for _,pod := range k8sCache.GetAllPods() {
		var podReqCPU int64
		var podReqMem int64
		var podLimitCPU int64
		var podLimitMem int64
		for _, container := range pod.Spec.Containers{
			val := container.Resources.Requests.Cpu().MilliValue()
			podReqCPU += val
			val = container.Resources.Requests.Memory().Value()
			podReqMem += val
			val = container.Resources.Limits.Cpu().MilliValue()
			podLimitCPU += val
			val = container.Resources.Limits.Memory().Value()
			podLimitMem += val
		}
		
		var ownerKinds []string
		var ownerNames []string
		for _,owner := range pod.OwnerReferences {
			ownerKinds = append(ownerKinds, owner.Kind)
			ownerNames = append(ownerNames, owner.Name)
		}
		
		podName   := pod.Name
		ownerName := strings.Join(ownerNames,"|")
		
		if shouldHash {
			podName = fmt.Sprintf("%x",md5.Sum([]byte(podName)))
			ownerName = fmt.Sprintf("%x",md5.Sum([]byte(ownerName)))
		}
		
		
		podsRecords = append(podsRecords, []string{
			podName,
			pod.Spec.NodeName,
			node2group[pod.Spec.NodeName],
			pod.Namespace,
			strings.Join(ownerKinds,"|"),
			ownerName,
			strconv.FormatInt(podReqCPU,10),
			strconv.FormatInt(podReqMem,10),
			strconv.FormatInt(podLimitCPU,10),
			strconv.FormatInt(podLimitMem,10),

		})
	}

	writer := csv.NewWriter(podsCsv)
	err = writer.WriteAll(podsRecords)
	if err != nil{
		log.Println("Failed writing pods csv")
	}

}

func printNodeGroups(k8sCache clustercache.ClusterCache) map[string]string{
	// initialize CSV file
	nodeGroupsCsv, err := os.Create("node_groups.csv")
	nodesCsv, err := os.Create("nodes.csv")

	defer nodeGroupsCsv.Close()
	defer nodesCsv.Close()

	if err != nil {
		log.Fatalln("failed to open node_groups.csv")
	}

	allNodes := k8sCache.GetAllNodes()
	nodeGroups := make(map[string][]*v1.Node)
	labelsStats := make(map[string]int)

	for _,node := range allNodes {		
		var nodeLabels []string
		for k,v := range node.Labels {
			labelsStats[k]++
			if !contains(ignoreValues, k) {
				nodeLabels = append(nodeLabels, k+":"+v)
			}
		}
		sort.Strings(nodeLabels)
		nodeSignature := strings.Join(nodeLabels,",")
		nodeGroups[nodeSignature] = append(nodeGroups[nodeSignature], node)
	}

	currGroup := 0

	// var csv_records [][]string
	groupNodesRecords := [][]string{
        {"group_id", "number_of_nodes", "unique_labels", "ignore_labels"},
	}
	nodesRecords := [][]string{
        {"group_id", "node_name", "taints", "cap_cpu_mili_core","cap_memory_byte", "alloc_cpu_mili_core", "alloc_bytes"},
	}

	node2group := make(map[string]string)

	for _, nodes := range nodeGroups {
		fmt.Println("===== Node group: " + strconv.Itoa(currGroup) + " ======" )
		ignoreLabels, uniqueLabels := printLabels(nodes[0].Labels, labelsStats, len(allNodes))

		groupNodesRecords = append(groupNodesRecords, []string{strconv.Itoa(currGroup),
															   strconv.Itoa(len(nodes)),
															   strings.Join(uniqueLabels," | "),
															   strings.Join(ignoreLabels," | "),
															})

		fmt.Println("Nodes:")
		for _,node := range nodes {
			node2group[node.Name] = strconv.Itoa(currGroup)
			node.Status.Capacity.Pods()
			allocCPU := node.Status.Allocatable.Cpu()
			allocMemory := node.Status.Allocatable.Memory()
			capCPU := node.Status.Capacity.Cpu()
			capMemory := node.Status.Capacity.Memory()
			
			taints := node.Spec.Taints
			var taintsNames []string
			for _,curr := range taints {
				
				taintsNames = append(taintsNames, fmt.Sprintf("%s:%s(%s)", curr.Key, curr.Value, curr.Effect))
			}
			fmt.Println(" * Name: ",node.Name,", node taints: ", strings.Join(taintsNames,","),", allocCPU: ", allocCPU, ", allocMemory", allocMemory, ", capCpu", capCPU, ", capMemory", capMemory)
			nodesRecords = append(nodesRecords, []string{
				strconv.Itoa(currGroup),
				node.Name, 
				strings.Join(taintsNames,","), 
				strconv.FormatInt(capCPU.MilliValue(),10), 
				strconv.FormatInt(capMemory.Value(),10),
				strconv.FormatInt(allocCPU.MilliValue(),10), 
				strconv.FormatInt(allocMemory.Value(),10),
				})
		}
		
		currGroup++
	}
	ngw := csv.NewWriter(nodeGroupsCsv)
	nw  := csv.NewWriter(nodesCsv)

	err = ngw.WriteAll(groupNodesRecords)
	if err != nil {
		log.Println("Faild writing node groups CSV")
	}
	err = nw.WriteAll(nodesRecords)
	if err != nil {
		log.Println("Faild writing nodes CSV")
	}

	return node2group
}

func printDeployments(k8sCache clustercache.ClusterCache){
	if len(k8sCache.GetAllDeployments()) > 0 {
		fmt.Println("============== Deployments =============")
	} else {
		return
	}

	for i, dep := range k8sCache.GetAllDeployments(){
		var nodeAffinity, podAffinity, podAntiAffinity = getAffinity(dep.Spec.Template.Spec.Affinity)
		fmt.Printf(" (%d) %s, Replicas: %d, Node Selector: %s, labelSelectors: %s\n", 
			i, 
			dep.Name, 
			dep.Spec.Replicas, 
			stringsMapToString(dep.Spec.Template.Spec.NodeSelector),
			stringsMapToString(dep.Spec.Selector.MatchLabels),
		)
		
		fmt.Printf("      NodeAffinity: %s, PodAffinity: %s, PodAntiAffinity: %s\n",
			nodeAffinity,
			podAffinity,
			podAntiAffinity)
		
		fmt.Println("      Containers:")
		for _, container := range dep.Spec.Template.Spec.Containers {
			fmt.Printf("            %s, request cpu: %v, request memory: %v\n", container.Name, container.Resources.Requests.Cpu().String(), container.Resources.Requests.Memory().String())
		}
		
		if len(dep.Spec.Selector.MatchExpressions) > 0 {
			fmt.Println("  expression selectors:")
		}
		for _,exprSelector := range dep.Spec.Selector.MatchExpressions {
			fmt.Printf("     %s:%s\n", exprSelector.Key, strings.Join(exprSelector.Values, ","))
		}
	}
}

func printStatefulSets(k8sCache clustercache.ClusterCache){
	statefulSets := k8sCache.GetAllStatefulSets()
	if len(statefulSets) > 0 {
		fmt.Println("============== Stateful States =============")
	} else {
		return
	}
	for i, sts := range statefulSets {
		var nodeAffinity, podAffinity, podAntiAffinity = getAffinity(sts.Spec.Template.Spec.Affinity)
		selector :=sts.Spec.Selector	
		fmt.Printf("  (%d) %s, Replicas: %d, Pod management policy: %s, labelSelectors: %s\n", 
			i, 
			sts.Name, 
			*sts.Spec.Replicas, 
			sts.Spec.PodManagementPolicy, 
			stringsMapToString(selector.MatchLabels))


		fmt.Printf("      NodeAffinity: %s, PodAffinity: %s, PodAntiAffinity: %s\n",
			nodeAffinity,
			podAffinity,
			podAntiAffinity)
		fmt.Println("      Containers:")
		for _, container := range sts.Spec.Template.Spec.Containers {
			fmt.Printf("            %s, request cpu: %v, request memory: %v\n", container.Name, container.Resources.Requests.Cpu().Format, container.Resources.Requests.Memory().Format)
		}
		
		if len(selector.MatchExpressions) > 0 {
			fmt.Println("      expression selectors:")
		}
		for _,exprSelector := range selector.MatchExpressions {
			fmt.Printf("            %s:%s\n", exprSelector.Key, strings.Join(exprSelector.Values, ","))
		}
	}
}

func printDaemonSets(k8sCache clustercache.ClusterCache){
	daemonSets := k8sCache.GetAllDaemonSets()

	if len(daemonSets) > 0 {
		fmt.Println("============== Daemon Sets =============")
	} else {
		return
	}
	for i, ds := range daemonSets {
		selector :=ds.Spec.Selector	

		var nodeAffinity, podAffinity, podAntiAffinity = getAffinity(ds.Spec.Template.Spec.Affinity)

		fmt.Printf(" (%d) %s, labelSelectors: %s\n", 
			i, 
			ds.Name, 
			stringsMapToString(selector.MatchLabels))

		fmt.Printf("      NodeAffinity: %s, PodAffinity: %s, PodAntiAffinity: %s\n",
			nodeAffinity,
			podAffinity,
			podAntiAffinity)
		
			fmt.Println("      Containers: ")
		for _, container := range ds.Spec.Template.Spec.Containers {
			fmt.Printf("            %s, request cpu: %v, request memory: %v\n", container.Name, container.Resources.Requests.Cpu().Format, container.Resources.Requests.Memory().Format)
		}

		if len(selector.MatchExpressions) > 0 {
			fmt.Println("      expression selectors:")
		}
		for _,exprSelector := range selector.MatchExpressions {
			fmt.Printf("            %s:%s\n", exprSelector.Key, strings.Join(exprSelector.Values, ","))
		}
	}
}

func stringsMapToString(labels map[string]string) string{
	var labelsSlice []string
	for k,v := range labels {
		labelsSlice = append(labelsSlice, k +":"+v)
	}
	sort.Strings(labelsSlice)
	return strings.Join(labelsSlice,",")
}

func contains(arr []string, str string) bool {
	for _, a := range arr {
	   if a == str {
		  return true
	   }
	}
	return false
 }


func printLabels(labels map[string]string, labelsStats map[string]int, numOfNodes int) (ignoreLbls []string, uniqueLbls []string){
	fmt.Println("labels:")
	var keys []string
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var commonLabels []string
	var ignoreLabels []string
	var uniqueLabels []string
	for _,key := range keys{
		if (contains(ignoreValues, key)) {
			ignoreLabels = append(ignoreLabels, key)
		}
		if(labelsStats[key] == numOfNodes) {
			commonLabels = append(commonLabels, key + " : " + labels[key])
		} else {
			uniqueLabels = append(uniqueLabels, key)
		}
	}


	fmt.Println(" * common labels: ", strings.Join(commonLabels,","))
	fmt.Println(" * ignore labels (not participating in group calculation):")
	for _,key := range ignoreLabels {
		fmt.Println("    * ",key,":",labels[key])
	}
	fmt.Println(" * unique labels: ")
	for _,key := range uniqueLabels {
		fmt.Println("    * ",key,":",labels[key])
	}

	return ignoreLabels, uniqueLabels
}

func getAffinity(affinity *v1.Affinity) (nodeAffinity, podAffinity, podAntiAffinity string){
	if affinity != nil {
		if affinity.NodeAffinity != nil {
			nodeAffinity = affinity.NodeAffinity.String()
		}

		if affinity.NodeAffinity != nil {
			podAffinity = affinity.PodAffinity.String()
		}

		if affinity.NodeAffinity != nil {
			podAntiAffinity = affinity.PodAntiAffinity.String()
		}
	}
	return nodeAffinity,podAffinity,podAntiAffinity
}
