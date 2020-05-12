# About

ProduceWithCamel is an experimental processor for [Apache Nifi](http://nifi.apache.org/) that uses [Apache Camel](http://camel.apache.org/) to
produce messages.  

The processor is based on [Assimbly Connector](https://github.com/assimbly/connector)

### Installation

1. [Download](https://github.com/assimbly/producewithcamel/releases) the NAR file.
2. Put the NAR file in the lib directory of Nifi.
3. For older installations of Nifi (before version 1.9) you need to restart.

### Usage

The processor has 5 properties:

* To URI: The URI of the Camel component.
* Error URI: The URI of the Camel componet in case of an error
* Maximum Deliveries: Maximum of retries in case of an error 
* Delivery Delay: Delay between retries
* Log Level: The loglevel of the Camel route

For the URI format of the camel componet see [Camel's component reference](https://camel.apache.org/components/latest/)