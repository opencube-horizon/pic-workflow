FROM debian:stable-slim as base
WORKDIR /pic/

# install binaries 
RUN apt-get update \
    && apt-get install -y libopenmpi3 openmpi-bin openmpi-common libopenblas0 libhdf5-cpp-103-1 libgomp1 python3-minimal

# create an intermediary builder, with development packages
FROM base AS builder
RUN apt-get install -y libopenmpi-dev build-essential git cmake libopenblas-dev libhdf5-dev libhdf5-openmpi-dev

RUN git clone https://github.com/KTH-HPC/sputniPIC.git sputniPIC \
    && cd sputniPIC \
    && sed -i '1s/^/#include <mpi.h>\n /' src/RW_IO.cpp \
    && cmake -DCMAKE_C_COMPILER=mpicc . \
    && make -j 8 

# create a stripped image, with only required data
FROM base
COPY --from=builder /pic/sputniPIC/sputniPIC_CPU.out /pic/sputniPIC
COPY --from=builder /pic/sputniPIC/inputfiles        /pic/inputfiles
COPY scripts /pic/scripts/
