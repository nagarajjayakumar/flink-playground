Job configuration parameters are specified in the job.properties file.

## Usage on Secured  Cluster

To run the command on secured SASL_SSL (Kerberos) cluster:

```
flink run -yD security.kerberos.login.keytab=<keytab file> -yD security.kerberos.login.principal=<principal_name> -d -p 1 -ys 2 -ynm StreamingKafkaJob target/flink-playground.jar config/job.properties
```

## Usage on Unsecured Cluster

```
flink run -d -p 1 -ys 1 -ytm 1500 -ynm StreamingKafkaJob target/flink-playground.jar config/job.properties
```
To fully control the resource utilization of the Flink job, we set the following CLI parameters:

```
-p 8: Parallelism of your pipeline. Controls the number of parallel instances of each operator.
-ys 4: Number of task slots in each TaskManager. It also determines the number of TaskManagers as the result of dividing the parallelism by the number of task slots.
-ytm 1500: TaskManager container memory size that ultimately defines how much memory can be used for heap, network buffers and local state management.
```

## Usage of Pyflink Job submission 

```
flink run   -d   -t yarn-per-job    -pyarch venv.zip   -pyclientexec venv.zip/venv/bin/python3.8 -pyexec venv.zip/venv/bin/python3.8  -py /tmp/nag_test.py 

```
