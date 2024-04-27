

#include <stdio.h>
#include <assert.h>
#include <string.h>

#include <sys/socket.h>
#include <unistd.h>
#include <arpa/inet.h>

#include <stdlib.h>
#include <sys/time.h>



#define MAX_MAG_LENGTH		1024
#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)


int connect_kvstore(const char *ip, int port) {

	int connfd = socket(AF_INET, SOCK_STREAM, 0);

	struct sockaddr_in kvs_addr;
	memset(&kvs_addr, 0, sizeof(struct sockaddr_in));

	kvs_addr.sin_family = AF_INET;
	kvs_addr.sin_addr.s_addr = inet_addr(ip);
	kvs_addr.sin_port = htons(port);

	int ret = connect(connfd, (struct sockaddr*)&kvs_addr, sizeof(struct sockaddr_in));
	if (ret) {
		perror("connect\n");
		return -1;
	}
	
	return connfd;
}

int send_msg(int connfd, char *msg) {
	int res = send(connfd, msg, strlen(msg), 0);
	if (res < 0) {
		exit(1);
	}

	return res;
}


int recv_msg(int connfd, char *msg) {

	int res = recv(connfd, msg, MAX_MAG_LENGTH, 0);
	if (res < 0) {
		exit(1);
	}
	return res;
}

void equals(char *pattern, char *result, char *casename) {

	
	if (0 == strcmp(pattern, result)) {
		//printf(">> PASS --> %s \n", casename);
	} else {
		printf(">> FAILED --> '%s' != '%s'\n", pattern, result);
	}
	
}


void test_case(int connfd, char *cmd, char *pattern, char *casename) {

	
	char result[MAX_MAG_LENGTH] = {0};

	send_msg(connfd, cmd);
	recv_msg(connfd, result);
	
	equals(pattern, result, casename);

}

void array_testcase(int connfd) {

	
	test_case(connfd, "SET Name King", "SUCCESS\r\n", "SetNameCase");

	test_case(connfd, "COUNT", "1\r\n", "COUNTCase");

	test_case(connfd, "SET Content-Encoding gzip", "SUCCESS\r\n", "SetNameCase");
	test_case(connfd, "COUNT", "2\r\n", "COUNT");

	test_case(connfd, "SET Content-Type text/html;charset=utf-8", "SUCCESS\r\n", "SetNameCase");
	test_case(connfd, "COUNT", "3\r\n", "COUNT");

	test_case(connfd, "SET Server Nginx", "SUCCESS\r\n", "SetNameCase");
	test_case(connfd, "COUNT", "4\r\n", "COUNT");

	test_case(connfd, "SET Vary Accept-Encoding", "SUCCESS\r\n", "SetNameCase");
	test_case(connfd, "COUNT", "5\r\n", "COUNT");
	
	test_case(connfd, "EXIST Name", "1\r\n", "EXISTCase");
	test_case(connfd, "GET Name", "King\r\n", "GetNameCase");
	test_case(connfd, "DELETE Name", "SUCCESS\r\n", "DELETECase");
	test_case(connfd, "COUNT", "4\r\n", "COUNT");
	test_case(connfd, "EXIST Name", "0\r\n", "EXISTCase");

	test_case(connfd, "EXIST Content-Encoding", "1\r\n", "EXISTCase");
	test_case(connfd, "GET Content-Encoding", "gzip\r\n", "GetNameCase");
	test_case(connfd, "DELETE Content-Encoding", "SUCCESS\r\n", "DELETECase");
	test_case(connfd, "COUNT", "3\r\n", "COUNT");

	test_case(connfd, "EXIST Content-Type", "1\r\n", "EXISTCase");
	test_case(connfd, "GET Content-Type", "text/html;charset=utf-8\r\n", "GetNameCase");
	test_case(connfd, "DELETE Content-Type", "SUCCESS\r\n", "DELETECase");
	test_case(connfd, "COUNT", "2\r\n", "COUNT");

	test_case(connfd, "EXIST Server", "1\r\n", "EXISTCase");
	test_case(connfd, "GET Server", "Nginx\r\n", "GetNameCase");
	test_case(connfd, "DELETE Server", "SUCCESS\r\n", "DELETECase");
	test_case(connfd, "COUNT", "1\r\n", "COUNT");

	test_case(connfd, "EXIST Server", "0\r\n", "EXISTCase");

	test_case(connfd, "EXIST Vary", "1\r\n", "EXISTCase");
	test_case(connfd, "GET Vary", "Accept-Encoding\r\n", "GetNameCase");
	test_case(connfd, "DELETE Vary", "SUCCESS\r\n", "DELETECase");
	test_case(connfd, "COUNT", "0\r\n", "COUNT");


}



void rbtree_testcase(int connfd) {
	
	test_case(connfd, "RSET Name King", "SUCCESS\r\n", "RSetNameCase");

	test_case(connfd, "RCOUNT", "1\r\n", "RCOUNTCase");

	test_case(connfd, "RSET Content-Encoding gzip", "SUCCESS\r\n", "RSetNameCase");
	test_case(connfd, "RCOUNT", "2\r\n", "RCOUNT");

	test_case(connfd, "RSET Content-Type text/html;charset=utf-8", "SUCCESS\r\n", "RSetNameCase");
	test_case(connfd, "RCOUNT", "3\r\n", "RCOUNT");

	test_case(connfd, "RSET Server Nginx", "SUCCESS\r\n", "RSetNameCase");
	test_case(connfd, "RCOUNT", "4\r\n", "RCOUNT");

	test_case(connfd, "RSET Vary Accept-Encoding", "SUCCESS\r\n", "SetNameCase");
	test_case(connfd, "RCOUNT", "5\r\n", "COUNT");
	
	test_case(connfd, "REXIST Name", "1\r\n", "EXISTCase");
	test_case(connfd, "RGET Name", "King\r\n", "GetNameCase");
	test_case(connfd, "RDELETE Name", "SUCCESS\r\n", "DELETECase");
	test_case(connfd, "RCOUNT", "4\r\n", "COUNT");
	test_case(connfd, "REXIST Name", "0\r\n", "EXISTCase");

	test_case(connfd, "REXIST Content-Encoding", "1\r\n", "EXISTCase");
	test_case(connfd, "RGET Content-Encoding", "gzip\r\n", "GetNameCase");
	test_case(connfd, "RDELETE Content-Encoding", "SUCCESS\r\n", "DELETECase");
	test_case(connfd, "RCOUNT", "3\r\n", "COUNT");

	test_case(connfd, "REXIST Content-Type", "1\r\n", "EXISTCase");
	test_case(connfd, "RGET Content-Type", "text/html;charset=utf-8\r\n", "GetNameCase");
	test_case(connfd, "RDELETE Content-Type", "SUCCESS\r\n", "DELETECase");
	test_case(connfd, "RCOUNT", "2\r\n", "COUNT");

	test_case(connfd, "REXIST Server", "1\r\n", "EXISTCase");
	test_case(connfd, "RGET Server", "Nginx\r\n", "GetNameCase");
	test_case(connfd, "RDELETE Server", "SUCCESS\r\n", "DELETECase");
	test_case(connfd, "RCOUNT", "1\r\n", "COUNT");

	test_case(connfd, "REXIST Server", "0\r\n", "EXISTCase");

	test_case(connfd, "REXIST Vary", "1\r\n", "EXISTCase");
	test_case(connfd, "RGET Vary", "Accept-Encoding\r\n", "GetNameCase");
	test_case(connfd, "RDELETE Vary", "SUCCESS\r\n", "DELETECase");
	test_case(connfd, "RCOUNT", "0\r\n", "COUNT");


}



void hash_testcase(int connfd) {

	
	test_case(connfd, "HSET Name King", "SUCCESS\r\n", "SetNameCase");

	test_case(connfd, "HCOUNT", "1\r\n", "COUNTCase");

	test_case(connfd, "HSET Content-Encoding gzip", "SUCCESS\r\n", "SetNameCase");
	test_case(connfd, "HCOUNT", "2\r\n", "COUNT");

	test_case(connfd, "HSET Content-Type text/html;charset=utf-8", "SUCCESS\r\n", "SetNameCase");
	test_case(connfd, "HCOUNT", "3\r\n", "COUNT");

	test_case(connfd, "HSET Server Nginx", "SUCCESS\r\n", "SetNameCase");
	test_case(connfd, "HCOUNT", "4\r\n", "COUNT");

	test_case(connfd, "HSET Vary Accept-Encoding", "SUCCESS\r\n", "SetNameCase");
	test_case(connfd, "HCOUNT", "5\r\n", "COUNT");
	
	test_case(connfd, "HEXIST Name", "1\r\n", "EXISTCase");
	test_case(connfd, "HGET Name", "King\r\n", "GetNameCase");
	test_case(connfd, "HDELETE Name", "SUCCESS\r\n", "DELETECase");
	test_case(connfd, "HCOUNT", "4\r\n", "COUNT");
	test_case(connfd, "HEXIST Name", "0\r\n", "EXISTCase");

	test_case(connfd, "HEXIST Content-Encoding", "1\r\n", "EXISTCase");
	test_case(connfd, "HGET Content-Encoding", "gzip\r\n", "GetNameCase");
	test_case(connfd, "HDELETE Content-Encoding", "SUCCESS\r\n", "DELETECase");
	test_case(connfd, "HCOUNT", "3\r\n", "COUNT");

	test_case(connfd, "HEXIST Content-Type", "1\r\n", "EXISTCase");
	test_case(connfd, "HGET Content-Type", "text/html;charset=utf-8\r\n", "GetNameCase");
	test_case(connfd, "HDELETE Content-Type", "SUCCESS\r\n", "DELETECase");
	test_case(connfd, "HCOUNT", "2\r\n", "COUNT");

	test_case(connfd, "HEXIST Server", "1\r\n", "EXISTCase");
	test_case(connfd, "HGET Server", "Nginx\r\n", "GetNameCase");
	test_case(connfd, "HDELETE Server", "SUCCESS\r\n", "DELETECase");
	test_case(connfd, "HCOUNT", "1\r\n", "COUNT");

	test_case(connfd, "HEXIST Server", "0\r\n", "EXISTCase");

	test_case(connfd, "HEXIST Vary", "1\r\n", "EXISTCase");
	test_case(connfd, "HGET Vary", "Accept-Encoding\r\n", "GetNameCase");
	test_case(connfd, "HDELETE Vary", "SUCCESS\r\n", "DELETECase");
	test_case(connfd, "HCOUNT", "0\r\n", "COUNT");
	


}



#define TEST_COUNT		100000

void rbtree_testcase_10w(int connfd) {

	int i;

	for (i = 0;i < TEST_COUNT;i ++) {
		char msg[512] = {0};
		snprintf(msg, 512, "RSET Key%d Value%d", i, i);

		char casename[64] = {0};
		snprintf(casename, 64, "casename%d", i);
		test_case(connfd, msg, "SUCCESS\r\n", casename);
	}

}

void hash_testcase_10w(int connfd) {

	int i;

	for (i = 0;i < TEST_COUNT;i ++) {
		char msg[512] = {0};
		snprintf(msg, 512, "HSET Key%d Value%d", i, i);

		char casename[64] = {0};
		snprintf(casename, 64, "casename%d", i);
		test_case(connfd, msg, "SUCCESS\r\n", casename);
	}

}


// assert();
// testcast ip port
int main(int argc, char *argv[]) {

	

	if (argc != 3) {
		printf("argc != 3\n");
		return -1;
	}

	const char *ip = argv[1];
	int port = atoi(argv[2]);

	int connfd = connect_kvstore(ip, port);
#if 1
	// array
	printf(" -----> array testcase <-------\n");
	array_testcase(connfd);

	printf(" -----> rbtree testcase <-------\n");
	rbtree_testcase(connfd);

	printf(" -----> hash testcase <-------\n");
	hash_testcase(connfd);
#endif

#if 1
	printf(" -----> rbtree testcase 10w <-------\n");

	struct timeval rbtree_begin;
	gettimeofday(&rbtree_begin, NULL);

	rbtree_testcase_10w(connfd);

	struct timeval rbtree_end;
	gettimeofday(&rbtree_end, NULL);

	int rbtime_used = TIME_SUB_MS(rbtree_end, rbtree_begin);
	

	printf("time_used: %d, qps: %d (request/second)\n", rbtime_used, (TEST_COUNT / rbtime_used) * 1000);
#endif

#if 1
	printf(" -----> rbtree testcase 10w <-------\n");

	struct timeval hash_begin;
	gettimeofday(&hash_begin, NULL);

	hash_testcase_10w(connfd);

	struct timeval hash_end;
	gettimeofday(&hash_end, NULL);

	int hashtime_used = TIME_SUB_MS(hash_end, hash_begin);
	
	printf("time_used: %d, qps: %d (request/second)\n", hashtime_used, (TEST_COUNT / hashtime_used) * 1000);
#endif


	// rbtree
#if 1
	test_case(connfd, "SET Name King", "SUCCESS\r\n", "setname");
	test_case(connfd, "GET Name", "King\r\n", "Getname");
	test_case(connfd, "COUNT", "1\r\n", "COUNT");
	test_case(connfd, "DELETE Name", "SUCCESS\r\n", "DELETECase");
	//test_case(connfd, "REXIST Name", "1\r\n", "EXIST");
#endif
	close(connfd);
	
}



