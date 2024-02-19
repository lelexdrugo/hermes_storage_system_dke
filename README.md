# Hermes Storage System
hermes storage system as presented for DKE paper.


## Setup
Test environment characteristics, presented to facilitate the reproducibility of the experiments.
### Docker images
| Service         | Image                     | Ver    |
|-----------------|---------------------------|--------|
| MongoDB         | mongo                     | 6      |
| Apache Kafka    | confluentinc/cp-kafka     | 6.0.14 |
| Apache Zookeper | confluentinc/cp-zookeeper | 6.0.14 |

### Microservices specification
| Major dependency                | Hermes         | Client producer |
|---------------------------------|----------------|-----------------|
| org.springframework.boot        | 2.7.5          | 2.7.5           |
| io.spring.dependency-management | 1.0.15.RELEASE | 1.0.15.RELEASE  |
| kotlin                          | 1.6.21         | 1.6.21          |
| Resource allocation             |                |                 |
| Reserved RAM                    | 16GB           | 12GB            |

### Testbed server
| Element             | Detail                                       |
|---------------------|----------------------------------------------|
| docker engine       | 24.0.7                                       |
| docker compose      | 2.21.0                                       |
| docker compose file | 3.9                                          |
| operating system    | Ubuntu 22.04.3 LTS                           |
| CPU                 | 6-core Intel(R) Xeon(R) CPU E-2136 @ 3.30GHz |
| RAM                 | 4x16GB Samsung M391A2K43BB1-CTD DDR4 DRAM    |
| Storage (SSD NVME)  |  Western Digital CL SN720 SDAQNTW-512G-2000  |
