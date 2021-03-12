package main

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"

	"github.com/aws/aws-sdk-go/aws/awserr"
	// "github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/pricing"
	// "github.com/aws/aws-sdk-go/service/ec2"

	// "github.com/aws/aws-sdk-go/service/s3"
)

// Uploads a file to S3 given a bucket and object key. Also takes a duration
// value to terminate the update if it doesn't complete within that time.
//
// The AWS Region needs to be provided in the AWS shared config or on the
// environment variable as `AWS_REGION`. Credentials also must be provided
// Will default to shared config file, but can load from environment if provided.
//
// Usage:
//   # Upload myfile.txt to myBucket/myKey. Must complete within 10 minutes or will fail
//   go run withContext.go -b mybucket -k myKey -d 10m < myfile.txt
func main() {
	// var bucket, key string
	// var timeout time.Duration

	// flag.StringVar(&bucket, "b", "", "Bucket name.")
	// flag.StringVar(&key, "k", "", "Object key name.")
	// flag.DurationVar(&timeout, "d", 0, "Upload timeout.")
	// flag.Parse()

	// All clients require a Session. The Session provides the client with
	// shared configuration such as region, endpoint, and credentials. A
	// Session should be shared where possible to take advantage of
	// configuration and credential caching. See the session package for
	// more information.

	// sess := session.Must(session.NewSession())

	// sess := session.Must(session.NewSessionWithOptions(session.Options{
	// 							SharedConfigState: session.SharedConfigEnable,
	// 							Profile: "ps-test",
	// 						}))

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(endpoints.UsEast1RegionID),
		Credentials: credentials.NewSharedCredentials("", "ps-account"),
	})

	if err != nil {
		panic(err.Error())
	}
	cred, err := sess.Config.Credentials.Get()
	fmt.Printf("credentials: %v\n", cred)

	if err != nil {
		panic(err.Error())
	}

	pricingURL := "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/"
	pricingURL += "index.json"


	svc := pricing.New(sess)
	input := &pricing.DescribeServicesInput{
		FormatVersion: aws.String("aws_v1"),
		MaxResults:    aws.Int64(1),
		ServiceCode:   aws.String("AmazonEC2"),
	}

	result, err := svc.DescribeServices(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case pricing.ErrCodeInternalErrorException:
				fmt.Println(pricing.ErrCodeInternalErrorException, aerr.Error())
			case pricing.ErrCodeInvalidParameterException:
				fmt.Println(pricing.ErrCodeInvalidParameterException, aerr.Error())
			case pricing.ErrCodeNotFoundException:
				fmt.Println(pricing.ErrCodeNotFoundException, aerr.Error())
			case pricing.ErrCodeInvalidNextTokenException:
				fmt.Println(pricing.ErrCodeInvalidNextTokenException, aerr.Error())
			case pricing.ErrCodeExpiredNextTokenException:
				fmt.Println(pricing.ErrCodeExpiredNextTokenException, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return
	}

	fmt.Println(result)
}
