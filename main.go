package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"log"
	"os"
)

type Item struct {
	Year   int
	Title  string
}

func init() {
	viper.SetConfigType("toml")
	viper.SetConfigName("config") // name of config file (without extension)

	path, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	viper.AddConfigPath(path)
	err = viper.ReadInConfig() // Find and read the config file
	if err != nil {
		log.Fatal(err)
	}

	viper.OnConfigChange(func(e fsnotify.Event) {
		err = viper.ReadInConfig() // Find and read the config file
		if err != nil {
			log.Fatal(err)
		}
	})

	viper.WatchConfig()
}
func main() {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-west-2"),
		Credentials: credentials.NewStaticCredentials(viper.GetString("cred.accesskeyid"), viper.GetString("cred.secretaccesskey"), "")},
	)
	if err != nil {
		log.Fatal(err)
	}
	svc:=dynamodb.New(sess)
	var ch int
	fmt.Println("1.create:\n2.insert item:\nenter choice:")
	fmt.Scanln(&ch)

	switch ch {

	case 1:Create(svc)

	case 2:Insert(svc)

	default:

	}
}

func Create(svc *dynamodb.DynamoDB)  {

	tableName := "Movies"

	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("Year"),
				AttributeType: aws.String("N"),
			},
			{
				AttributeName: aws.String("Title"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("Year"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("Title"),
				KeyType:       aws.String("RANGE"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
		TableName: aws.String(tableName),
	}

	_, err1 := svc.CreateTable(input)
	if err1 != nil {
		fmt.Println("Got error calling CreateTable:")
		fmt.Println(err1.Error())
		os.Exit(1)
	}

	fmt.Println("Created the table", tableName)
}

func Insert(svc *dynamodb.DynamoDB)  {
	tableName := "Movies"

	item:=Item{
		Year:2019,
		Title:"Baahubali",
	}

	av, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		fmt.Println("Got error marshalling new movie item:")
		fmt.Println(err.Error())
		os.Exit(1)
	}
	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(tableName),
	}

	_, err = svc.PutItem(input)
	if err != nil {
		fmt.Println("Got error calling PutItem:")
		fmt.Println(err.Error())
		os.Exit(1)
	}
}