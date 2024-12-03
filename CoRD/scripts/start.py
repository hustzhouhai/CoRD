# coding=utf-8

import os
import subprocess

filepath=os.path.realpath(__file__)
script_dir = os.path.dirname(os.path.normpath(filepath))
home_dir = os.path.dirname(os.path.normpath(script_dir))
conf_dir = home_dir+"/conf"
CONF = conf_dir+"/config.xml"
# print CONF
# read from configuration file of the slaves

f = open(CONF)
start = False
concactstr = ""
for line in f:
    if line.find("setting") == -1:
	line = line[:-1]
	concactstr += line
res=concactstr.split("<attribute>")

slavelist=[]
for attr in res:
    if attr.find("helpers.address") != -1:
	valuestart=attr.find("<value>")
	valueend=attr.find("</attribute>")
	attrtmp=attr[valuestart:valueend]
	slavestmp=attrtmp.split("<value>")
	for slaveentry in slavestmp:
	    if slaveentry.find("</value>") != -1:
		entrysplit=slaveentry.split("/")
	 	slaveentry=entrysplit[1]
		endpoint=slaveentry.find("</value>")
		slave=slaveentry[:endpoint]
		slavelist.append(slave)


# start

print "start coordinator"
os.system("redis-cli flushall")
os.system("killall ECCoordinator")
command="cd "+home_dir+"; ./ECCoordinator &> "+home_dir+"/coor_output &"
print command
subprocess.Popen(['/bin/bash', '-c', command])


for slave in slavelist:
    print "start slave on " + slave
    os.system("ssh " + slave + " \"killall ECHelper \"")
    os.system("ssh " + slave + " \"killall ECPipeClient \"")
    command="scp "+home_dir+"/ECHelper "+slave+":"+home_dir+"/"
    os.system(command)
    command="scp "+home_dir+"/ECPipeClient "+slave+":"+home_dir+"/"
    os.system(command)
    
    command = "ssh "+slave+" \"cd "+home_dir + "; sudo chmod 777 ./ECHelper\""
    os.system(command)

    os.system("ssh " + slave + " \"redis-cli flushall \"")

	# set bandwidth
    upload_bw = 1 * 1024 * 1024 
    download_bw = 1 * 1024 * 1024
    net_adapter = 'eth0'  
    command = 'ssh {0} "wondershaper -c -a {1}; wondershaper -a {1} -u {2} -d {3}"'.format(slave, net_adapter, upload_bw, download_bw)
    os.system(command)

    command="ssh "+slave+" \"cd "+home_dir+"; ./ECHelper &> "+home_dir+"/node_output &\""
    subprocess.Popen(['/bin/bash', '-c', command])
