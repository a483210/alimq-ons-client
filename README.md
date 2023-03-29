# aliyun rocketmq client native

---

### 这个库是什么东西？

这个库来自于[Ali ons-client 1.8.8.8.Final](https://mvnrepository.com/artifact/com.aliyun.openservices/ons-client/1.8.8.8.Final)的修改版本

主要是去除了`com.aliyun.openservices.shade`下内置的netty等三方库，并且将FastJson替换为Jackson

### 这个库有什么用？

这个库的主要作用是能够兼容Graalvm Native

原库内置了一些三方库使得Graalvm的配置文件失效，并且FastJson1目前没找到有效支持Graalvm的版本，所以做了特殊处理

配合[alimq-spring-boot-starter](https://github.com/a483210/alimq-spring-boot-starter)做Native的兼容

### 如何使用？
```xml
<dependency>
  <groupId>com.a483210</groupId>
  <artifactId>ons-client</artifactId>
  <version>1.8.8.8.Final</version>
</dependency>
```