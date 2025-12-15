#include <iostream>
#include <pthread.h>
#include <vector>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <fcntl.h>
#include <unordered_map>
#include <mutex>
#define nl '\n'
using namespace std;

mutex uinfo, conlock, shfile;

struct serverThreadArgs{
    char *tport;
    char *tip;
};

unordered_map<string, string> userInfo;

struct Connected{
    string username;
    string ip;
    string port;
};
unordered_map<string, Connected*> connections;

struct Group{
    vector<string> usernames;
    vector<string> requests;
    string group_owner;
    string group_id;
};
unordered_map<string, Group*> groups;

struct SharedFiles{
    string fileName;
    string hash;
    string owner;
    int noOfChunks;
    vector<string> users;
};
unordered_map<string, SharedFiles*> shf_info;

void *connection(void *args){
    int *newcon = static_cast<int*>(args);
    int csockfd = *newcon;
    delete newcon;
    char buff[1000];
    char input[1000];
    char dummy[1000];
    int bytes_read = read(csockfd, buff, 1000);
    buff[bytes_read] = '\0';
    //---------------------------------- Notify Tracker -------------------------------------------
    if(strcmp(buff, "notify") == 0){
        write(csockfd, "0", 1);
        bytes_read = read(csockfd, buff, 1000);
        buff[bytes_read] = '\0';
        string tempuser = string(strtok(buff, " "));
        string tempfile = string(strtok(NULL, " "));
        shfile.lock();
        shf_info[tempfile]->users.push_back(tempuser);
        shfile.unlock();
        close(csockfd);
        pthread_exit(NULL);
    }
    //---------------------------------------------------------------------------------------------


    //---------------------------------- Reading ip and port ---------------------------------------
    
    if(strcmp(buff, "hello") != 0){
        write(csockfd, "error", 5);
        pthread_exit(NULL);
    }
    else   
        write(csockfd, "send_ip_port", 12);
    bytes_read = read(csockfd, buff, 1000);
    buff[bytes_read] = '\0';
    string peerIP = string(strtok(buff, ":"));
    string peerPort = string(strtok(NULL, ":"));
    write(csockfd, "ok", 2);
    //-----------------------------------------------------------------------------------------------
    string user = "";

    while(1){
        bytes_read = read(csockfd, buff, 1000);
        buff[bytes_read] = '\0';
        strcpy(dummy, buff);
        char *cmd = strtok(dummy, " ");
        if(strcmp(cmd, "create_user") == 0){
            string username = string(strtok(NULL, " "));
            string password = string(strtok(NULL, " "));
            uinfo.lock();
            if(userInfo.find(username) != userInfo.end()){
                write(csockfd, "1", 1);
                uinfo.unlock();
                continue;
            }else{
                userInfo[username] = password;
                write(csockfd, "0", 1);
            }
            conlock.lock();
            connections[username] = NULL;
            conlock.unlock();
            uinfo.unlock();
        }
        else if(strcmp(cmd, "login") == 0){
            string username = string(strtok(NULL, " "));
            string password = string(strtok(NULL, " ")); 
            uinfo.lock();
            if(userInfo.find(username) == userInfo.end()){
                write(csockfd, "1", 1);
                uinfo.unlock();
                continue;
            }
            else if(userInfo[username] != password){
                write(csockfd, "2", 1);
                uinfo.unlock();
                continue;
            }
            uinfo.unlock();
            conlock.lock();
            if(connections[username] != NULL){
                write(csockfd, "3", 1);
                conlock.unlock();
                continue;
            }
            else{
                connections[username] = new Connected;
                connections[username]->ip = peerIP;
                connections[username]->port = peerPort;
                connections[username]->username = username;
                user = username;
                write(csockfd, "0", 1);
            }
            conlock.unlock();

        }
        else if(strcmp(cmd, "upload_file") == 0){
            string fileName = string(strtok(NULL, " "));
            shfile.lock();
            if(shf_info.find(fileName) != shf_info.end()){
                write(csockfd, "1", 1);
                shfile.unlock();
                continue;
            }
            else{
                write(csockfd, "0", 1);
            }
            bytes_read = read(csockfd, buff, 1000);
            buff[bytes_read] = '\0';
            string temp = string(buff);
            shf_info[fileName] = new SharedFiles;
            shf_info[fileName]->fileName = fileName;
            shf_info[fileName]->hash = temp.substr(0, 40);
            shf_info[fileName]->noOfChunks = stoi(temp.substr(40));
            shf_info[fileName]->owner = user;
            shf_info[fileName]->users.push_back(user);
            shfile.unlock();
            write(csockfd, "0", 1);
        }
        else if(strcmp(cmd, "download_file") == 0){
            string fileName = string(strtok(NULL, " "));
            shfile.lock();
            if(shf_info.find(fileName) == shf_info.end()){
                write(csockfd, "1", 1);
                shfile.unlock();
                continue;
            }
            else{
                string sendMsg = "";
                for(string x:shf_info[fileName]->users){
                    conlock.lock();
                    if(connections[x]!=NULL){
                        sendMsg += (connections[x]->username + ":" + connections[x]->ip + ":" + connections[x]->port + ";");
                    }
                    conlock.unlock();
                }
                if(sendMsg == ""){
                    write(csockfd, "2", 1);
                    shfile.unlock();
                    continue;
                }
                write(csockfd, sendMsg.c_str(), sendMsg.size());
                bytes_read = read(csockfd, buff, 1000);
                buff[bytes_read] = '\0';
                if(strcmp(buff, "0") == 0){
                    string hash = shf_info[fileName]->hash;
                    hash = hash + to_string(shf_info[fileName]->noOfChunks);
                    write(csockfd, hash.c_str(), hash.size());
                }
            }
            shfile.unlock();
        }
        else if(strcmp(cmd, "logout") == 0){
            conlock.lock();
            Connected *temp = connections[user];
            connections[user] = NULL;
            delete temp;
            user = "";
            conlock.unlock();
        }
    }
    close(csockfd);
    pthread_exit(NULL);
}

void *server(void *args){
    serverThreadArgs *threadArgs = static_cast<serverThreadArgs*>(args);
    socklen_t clilen;
    int ssockfd, csockfd, tportno;
    struct sockaddr_in serv_addr, cli_addr;
    struct in_addr saddr;
    ssockfd =  socket(AF_INET, SOCK_STREAM, 0);
    tportno = stoi(threadArgs->tport);
    if(inet_pton(AF_INET, threadArgs->tip, &saddr) <= 0){
        cout << "Error: Invalid Ip address" << endl;
        exit(1);
    }
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = saddr.s_addr; 
    serv_addr.sin_port = htons(tportno);
    int opt = 1;
    if (setsockopt(ssockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(int)) < 0) {
        perror("setsockopt");
        // Handle the error
    }
    if (bind(ssockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
        perror("bind error");
        exit(1);
    }
    listen(ssockfd,5);
    while((csockfd = accept(ssockfd, (struct sockaddr *) &cli_addr, &clilen)) > 0){
        pthread_t con;
        int *ptr = new int;
        *ptr = csockfd;
        pthread_create(&con, NULL, connection, static_cast<void*>(ptr));
        pthread_detach(con);
    }
    close(ssockfd);
    return NULL;
}


int main(int argc, char *argv[]){
    pthread_t serverThread;
    if(argc != 3){
        cout << "Error: ./tracker tracker_info.txt tracker_no" << nl;
        exit(1);
    }
    char *trackerPort;
    char *trackerIP;
    char *trackerNo;
    int fd = open(argv[1], O_RDONLY);
    if(fd == -1){
        cout << "Error can't open file" << endl;
        exit(1);
    }
    char buff[255];
    int bytesRead = read(fd, buff, 255);
    if(bytesRead == -1){
        cout << "Error" << endl;
        exit(1);
    }
    buff[bytesRead] = '\0';
    trackerNo = strtok(buff, " ");
    trackerPort = strtok(NULL, " ");
    trackerIP = strtok(NULL, " ");
    serverThreadArgs sdata;
    sdata.tip = strtok(trackerIP, "");
    sdata.tport = strtok(trackerPort, "");
    pthread_create(&serverThread, NULL, server, static_cast<void*>(&sdata));
    pthread_join(serverThread, NULL);
}








