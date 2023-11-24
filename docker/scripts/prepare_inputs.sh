#!/bin/sh

set -e

[ "$#" -lt 1 ] && echo "usage: $0 sim_name [sim_name [...]]" && exit 1

for sim_name in "$@"; do
    sim_dir="out/$sim_name/"
    sim_file="$sim_name.inp"

    # create output directory for simulation data
    mkdir -p $sim_dir 2> /dev/null 

    # create the simulation configuration from the template
    sed -e "s#SaveDirName.*#SaveDirName = $sim_dir#" \
        -e "s#RestartDirName.*#RestartDirName = $sim_dir#" \
	$sim_file > $sim_dir/sim.inp

    echo "prepared simulation $sim_name, in $sim_dir"
done
