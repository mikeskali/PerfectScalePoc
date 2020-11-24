package main

import (
	"fmt"
	"strings"

	"github.com/mikeskali/PerfectScalePoc/clustercache"
	"github.com/mikeskali/PerfectScalePoc/env"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

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

	nodes := k8sCache.GetAllNodes()
	
	nodeGroups := make(map[string][]*v1.Node)

	for _,node := range nodes {
		fmt.Println(node.Name)
		
		nodeLabels := make([]string, 0, len(node.Labels))
		for k,v := range node.Labels {
			nodeLabels = append(nodeLabels, k+":"+v)
		}
		nodeSignature := strings.Join(nodeLabels,",")
		nodeGroups[nodeSignature] = append(nodeGroups[nodeSignature], node)
	}

	for sign, nodes := range nodeGroups {
		fmt.Println("===== Node group: " + sign + " ======" )
		
		for _,node := range nodes {
			allocCPU := node.Status.Allocatable.Cpu()
			allocMemory := node.Status.Allocatable.Memory()
			capCPU := node.Status.Capacity.Cpu()
			capMemory := node.Status.Capacity.Memory()
			// fmt.Printf("= *(%d ) ")
			fmt.Println(" * Name: ",node.Name,", allocCPU: ", allocCPU, ", allocMemory", allocMemory, ", capCpu", capCPU, ", capMemory", capMemory)
		}
	}
	
	fmt.Println() 
	fmt.Println()
	fmt.Println()
	fmt.Println()
	fmt.Println()

	pods := k8sCache.GetAllPods()

	for _, pod := range pods {
		podNodeName := pod.Spec.NodeName

		var requestCpu int64 = 0
		var requestMemory int64 = 0
		
		for _, container := range pod.Spec.Containers {
			rCPU, exists := container.Resources.Requests.Cpu().AsInt64()
			if exists {
				requestCpu = requestCpu + rCPU
			}

			rMem, exists := container.Resources.Requests.Memory().AsInt64()
			if exists {
				requestMemory = requestMemory + rMem
			}
		}

		fmt.Println("pod name: ", pod.Name, "Node: ", podNodeName, "reqCpu: ", requestCpu, "reqMem: ", requestMemory)
	}
}
