import os, sys
if 'SUMO_HOME' in os.environ:
  print "true"
     #tools = os.path.join(os.environ['SUMO_HOME'], 'tools')
     #sys.path.append(tools)
else:
  print "false"   
     #sys.exit("please declare environment variable 'SUMO_HOME'")