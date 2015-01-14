#!/bin/bash
# THIS SCRIPT IS MAINTAINED FOR STANDALONE TESTING OF THE CNV CALLING SYSTEM
# THESE TASKS ARE EXECUTED THROUGH LUIGI/CELERY IN OUR PRODUCTION ENVIRONMENT
echo "$1 $2 $3" | tee args

# increase file limits
echo "color hard nofile 100000" | sudo tee -a /etc/security/limits.conf
echo "color soft nofile 100000" | sudo tee -a /etc/security/limits.conf
echo "session required pam_limits.so" | sudo tee -a /etc/pam.d/common-session

# log back in to color to ensure that limits have been applied
sudo su color -c "./run_inner_pipeline.sh $1 $2 $3 |& tee logfile"
