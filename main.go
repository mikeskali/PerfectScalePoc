package main

import (
	"fmt"

	"github.com/mikeskali/PerfectScalePoc/env"
	"github.com/mikeskali/PerfectScalePoc/clustercache"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/kubernetes"
)

func main() {
	fmt.Println("Let's optimize stuff")
	env.Set("KUBECONFIG_PATH","/Users/michael.sklyar/.kube/config")
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

	
	// []*v1.Node
	nodes := k8sCache.GetAllNodes()
	for _,node := range nodes {
		fmt.Println(node.Name)
		allocCpu := node.Status.Allocatable.Cpu()
		allocMemory := node.Status.Allocatable.Memory()
		capCpu := node.Status.Capacity.Cpu()
		capMemory := node.Status.Capacity.Memory()
		

		fmt.Println("allocCPU: ", allocCpu, "allocMemory", allocMemory, "capCpu", capCpu, "capMemory", capMemory)
	}

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
