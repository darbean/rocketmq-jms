# RocketMQ-JMS  


## Introduction
RocketMQ-JMS is an implement of JMS specification,taking Apache RocketMQ as broker.
Now newest jms specification supported is JMS2.0. 
Although JMS2.1 early draft published in Oct 2015, it has been withdrawn from J2EE 8.   

Java 7 should be used for building as JMS specification over 2.0 only support at least Java 7.

Now RocketMQ-JMS has no release version and is still under developing. Hopefully you can contribute a bit for it :-)

## RoadMap  
  **version 1.0.0**  
  
  **Milestone-1**  
   
  * Refactor and perfect Message Model.
  * Refactor and perfect Message Domain(p2p and pub/sub excluding temp/browser/requestor). 
  * Refactor and perfect Connection(setup/start/stop/metadata/exception/close).
  * Refactor and perfect Session(messageOrder/ack/serial/threadRestriction).
  * Refactor and perfect Producer(synchronous/asynchronous).
  * Refactor and perfect Consumer(synchronous/asynchronous).  
  
  *including but not limited content between bracket*  
  
  **Milestone-2** 
  
  * Implement simplified api added in JMS2.0. 
  * Complete temp/browser/requestor of Message Domain.
  * Implement selector which filter message sending to mqPullConsumer.
  * Think of Local Transaction(distinguished with Distributed Transaction) and implement it if possible.
  * Other missing feature/constrain.
  
## Feature must not include in newest RocketMQ-JMS release 
  1. Distributed Transaction.
  2. JMS application server facilities.
  3. Message could expire at any time in the future.
  

## Building
````
  cd RocketMQ-JMS   
  mvn clean install  
  ````  
  **run unit test:**  mvn test    
  
  **run integration test:**  mvn verify
  
## TODO
* Add guidelines for new contributors
* Add travis for Continuous Integration
