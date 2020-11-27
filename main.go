package main

import (
	"fmt"
	"strings"
	"strconv"
	"sort"

	"github.com/mikeskali/PerfectScalePoc/clustercache"
	"github.com/mikeskali/PerfectScalePoc/env"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var ignoreValues = []string{"kubernetes.io/hostname","topology.kubernetes.io/zone", "failure-domain.beta.kubernetes.io/zone","logzio/az"}

func main() {
	fmt.Println("Let's optimize stuff")
	kubCfgPath := env.Get("KUBECONFIG_PATH","")
	if kubCfgPath == "" {
		fmt.Println("KUBECONFIG_PATH not set, exiting")
		return
	}
	
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
		panic(err.Error())
	}

	// Create Kubernetes Cluster Cache + Watchers
	k8sCache := clustercache.NewKubernetesClusterCache(kubeClientset)
	k8sCache.Run()

	allNodes := k8sCache.GetAllNodes()
	nodeGroups := make(map[string][]*v1.Node)
	labelsStats := make(map[string]int)

	for _,node := range allNodes {		
		nodeLabels := make([]string, 0, len(node.Labels))
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
	for _, nodes := range nodeGroups {
		fmt.Println("===== Node group: " + strconv.Itoa(currGroup) + " ======" )
		printLabels(nodes[0].Labels, labelsStats, len(allNodes))

		fmt.Println("Nodes:")
		for _,node := range nodes {
			allocCPU := node.Status.Allocatable.Cpu()
			allocMemory := node.Status.Allocatable.Memory()
			capCPU := node.Status.Capacity.Cpu()
			capMemory := node.Status.Capacity.Memory()
			// fmt.Printf("= *(%d ) ")
			fmt.Println(" * Name: ",node.Name,", allocCPU: ", allocCPU, ", allocMemory", allocMemory, ", capCpu", capCPU, ", capMemory", capMemory)
		}
		currGroup++
	}
	
	fmt.Println() 
	fmt.Println()
	fmt.Println()
	fmt.Println()
	fmt.Println()

	// pods := k8sCache.GetAllPods()

	// for _, pod := range pods {
	// 	podNodeName := pod.Spec.NodeName

	// 	var requestCpu int64 = 0
	// 	var requestMemory int64 = 0
		
	// 	for _, container := range pod.Spec.Containers {
	// 		rCPU, exists := container.Resources.Requests.Cpu().AsInt64()
	// 		if exists {
	// 			requestCpu = requestCpu + rCPU
	// 		}

	// 		rMem, exists := container.Resources.Requests.Memory().AsInt64()
	// 		if exists {
	// 			requestMemory = requestMemory + rMem
	// 		}
	// 	}

	// 	fmt.Println("pod name: ", pod.Name, "Node: ", podNodeName, "reqCpu: ", requestCpu, "reqMem: ", requestMemory)
	// }
}

func contains(arr []string, str string) bool {
	for _, a := range arr {
	   if a == str {
		  return true
	   }
	}
	return false
 }


func printLabels(labels map[string]string, labelsStats map[string]int, numOfNodes int){
	fmt.Println("labels:")
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	commonLabels := make([]string, 0)
	ignoreLabels := make([]string, 0)
	uniqueLabels := make([]string, 0)
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
}
