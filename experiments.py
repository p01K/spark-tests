#!/usr/bin/python2.7
import socket
import sys
import subprocess
import os
import numpy
import re

def submit_spark_job(classname,args):
    spark_home   = os.environ['SPARK_HOME']
    submit       = "{}/bin/spark-submit".format(spark_home)
    hostname     = socket.gethostname()
    # spark_master = "spark://{}:7077".format(hostname)
    spark_master = "spark://{}:7077".format('131.114.136.218')
    target       = 'target/scala-2.10/spark-tests_2.10-1.0.jar'
    props        = "{}/conf/spark-defaults.conf".format(spark_home)
    fargs        = "--master {} {}".format(spark_master,args)
    command      = "{} --class {} --properties-file {} {} {}".format(submit,classname,props,target,fargs)

    output = run_command(command)
    # print output
    return output

def run_command(command):
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (output, err) = p.communicate()

    p_status = p.wait()
    if p_status != 0:
        print err
        sys.exit(1)

    return output

def statistics(results):
    assert len(results)>5
    stats=results[5:] #drop the first 5 runtimes for warming up
    array=numpy.array(stats)
    return [numpy.average(array),numpy.std(array)]

def grep_output(output):
    timearray=None
    for line in output.split('\n'):
        if re.search('time:',line):
            timearray=line.strip().split(" ")[1]
    assert timearray!=None

    return map(lambda s: float(s),timearray.split(','))

def compute_speedup(a,b):
    array1=map(lambda x: x[0],a)
    array2=map(lambda x: x[0],b)

    return numpy.divide(array2,array1)

def run_benchmark(algo,dsched,runs,partitions):
    nsched=4 #fixed for now
    result=[]
    for p in partitions:
        args  ="--algo {} --dist-sched {} --nsched {} --partitions {} --runs {}".format(algo,dsched,nsched,p,runs)
        output=submit_spark_job('Run',args)
        timea = grep_output(output)
        print timea

        s=statistics(timea)
        # print s
        result.append(s)
    return result

def run_benchmark_elems(algo,dsched,runs,partitions,elements):
    nsched=4 #fixed for now
    result=[]
    p=partitions
    for e in elements:
        args  ="--algo {} --dist-sched {} --nsched {} "\
               "--partitions {} --runs {} --nelems {} ".format(algo,dsched,nsched,p,runs,e)
        output=submit_spark_job('Run',args)
        timea = grep_output(output)
        print timea
        s=statistics(timea)
        # print s
        result.append(s)
    return result


def whole_run(algo):
    print "with distributed scheduling"

    partitions=[32,64,128,256,512,1024,2048]

    # partitions=[16,32,64,128,256,512]#,1024,2048,4096,8192]

    results_true=run_benchmark(algo,'true',15,partitions)

    print "with default scheduling"
    results_false=run_benchmark(algo,'false',15,partitions)

    speedup = compute_speedup(results_true,results_false)

    array = zip(partitions,results_true,results_false,speedup)

    for e in array:
        print "{} & {:.2f} & {:.2f} & {:.2f} & {:.2f} &{:.2f} \\\\".format(e[0],e[1][0],e[1][1],e[2][0],e[2][1],e[3])

def whole_run_elements(algo):
    print "with distributed scheduling"
    partitions=512
    elements=[250000,1000000,4000000,16000000,64000000]
    results_true=run_benchmark_elems(algo,'true',15,partitions,elements)

    print "with default scheduling"
    results_false=run_benchmark_elems(algo,'false',15,partitions,elements)

    speedup = compute_speedup(results_true,results_false)

    array = zip(elements,results_true,results_false,speedup)
    
    for e in array:
        print "{} & {:.2f} & {:.2f} & {:.2f} & {:.2f} &{:.2f} \\\\".format(e[0],e[1][0],e[1][1],e[2][0],e[2][1],e[3])



whole_run("Filter33")
whole_run("ReducePlus")
whole_run("Collect")
whole_run("LongTail")

        
# whole_run("Filter33")
# whole_run("ReducePlus")
# whole_run("Collect")
# whole_run("LongTail")
whole_run("WordCount")
# whole_run_elements("Filter33")
# whole_run_elements("ReducePlus")
# whole_run_elements("Collect")
# whole_run_elements("LongTail")


# algo   = "Filter33"
# dsched = 'true'
# nsched = 4
# part   = 512
# runs   = 15

# args  ="--algo {} --dist-sched {} --nsched {} --partitions {} --runs {}".format(algo,dsched,nsched,part,runs)

# output=submit_spark_job('Run',args)
# timea = grep_output(output)
# # print args
# # print output
# print timea
# print statistics(timea)

# dsched= 'false'
# args  ="--algo {} --dist-sched {} --partitions {} --runs {}".format(algo,dsched,part,runs)
# output=submit_spark_job('Run',args)
# timea = grep_output(output)
# # print args
# # print output
# print timea
# print statistics(timea)
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
