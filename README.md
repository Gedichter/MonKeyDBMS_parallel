# MonKeyDBMS_parallel


Tip:

How to split a workload into multiple for multithreading: Use AWK

Example:\
awk 'NR%8==0' "$1" > 8thread_0.txt\
awk 'NR%8==1' "$1" > 8thread_1.txt\
awk 'NR%8==2' "$1" > 8thread_2.txt\
awk 'NR%8==3' "$1" > 8thread_3.txt\
awk 'NR%8==4' "$1" > 8thread_4.txt\
awk 'NR%8==5' "$1" > 8thread_5.txt\
awk 'NR%8==6' "$1" > 8thread_6.txt\
awk 'NR%8==7' "$1" > 8thread_7.txt\

