#!/bin/bash

vim $S4_IMAGE/s4-core/logs/s4-core/s4-core_`pgrep -f 'java -server'`.log
