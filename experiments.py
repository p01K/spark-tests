#!/usr/bin/python2.7
import subprocess

def run_command(command):
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (output, err) = p.communicate()
 
    p_status = p.wait()
    print "Command output : ", output
    return output

run_command("./submit.sh Filter '512 true 4'")
