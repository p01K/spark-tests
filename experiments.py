#!/usr/bin/python2.7
import socket
import sys
import subprocess
import os

def submit_spark_job(classname,args):
    spark_home   = os.environ['SPARK_HOME']
    submit       = "{}/bin/spark-submit".format(spark_home)
    hostname     = socket.gethostname()
    spark_master = "spark://{}:7077".format(hostname)
    target       = 'target/scala-2.10/spark-tests_2.10-1.0.jar'
    props        = "{}/conf/spark-defaults.conf".format(spark_home)
    fargs        = "--master {} {}".format(spark_master,args)
    command      = "{} --class {} --properties-file {} {} {}".format(submit,classname,props,target,fargs)

    output = run_command(command)
    return output
    
def run_command(command):
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (output, err) = p.communicate()

    p_status = p.wait()
    if p_status != 0:
        print err
        sys.exit(1)

    return output


# master=run_command("hostname")
algo   = "Filter33"
dsched = 'true'
nsched = 4
part   = 512
runs   = 10

args  ="--algo {} --dist-sched {} --nsched {} --partitions {} --runs {}".format(algo,dsched,nsched,part,runs)

output=submit_spark_job('Run',args)
print output
# output=run_command("./submit.sh Run '{}'".format(args))
# print output
# print args
# print output

# runs  = 10
# dsched= 'false'
# args  ="--algo {} --dist-sched {} --partitions {} --runs {}".format(algo,dsched,part,runs)
# output=run_command("./submit.sh Run '{}'".format(args))
# print args
# print output

# algo  = "ReducePlus"
# dsched= 'true'
# args  ="--algo {} --dist-sched {} --nsched {} --partitions {} --runs {}".format(algo,dsched,nsched,part,runs)
# output=run_command("./submit.sh Run '{}'".format(args))
# print args
# print output

# dsched= 'false'
# args  ="--algo {} --dist-sched {} --partitions {} --runs {}".format(algo,dsched,part,runs)
# output=run_command("./submit.sh Run '{}'".format(args))
# print args
# print output

# algo  = 'Collect'
# dsched= 'true'
# args  ="--algo {} --dist-sched {} --nsched {} --partitions {} --runs {}".format(algo,dsched,nsched,part,runs)
# output=run_command("./submit.sh Run '{}'".format(args))
# print args
# print output

# dsched= 'false'
# args  ="--algo {} --dist-sched {} --partitions {} --runs {}".format(algo,dsched,part,runs)
# output=run_command("./submit.sh Run '{}'".format(args))
# print args
# print output


# print err
