#include <iostream>
#include <pthread.h>
#include <vector>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <fcntl.h>
#include <openssl/sha.h>
#include <unordered_map>
#include <mutex>
#include <queue>
#define CHUNK_SIZE 64000
#define nl '\n'
#define NUM_THREADS 10
using namespace std;

mutex shflock, fdlock;
pthread_mutex_t tqmutex, dsmutex;
pthread_cond_t condQueue;
string gtrackerip = "", gtrackerport = "", curr_user = "";
struct userThreadArgs{
    char *tport;
    char *tip;
    char *cport;
    char *cip;
};
struct serverThreadArgs{
    char *cport;
    char *cip;
};

struct SharedFiles{
   string fileName;
   string filePath;
   string chunks; 
};
unordered_map<string, SharedFiles*> shinfo;

struct downloadFileInfo{
    string peers;
    string hash;
    string fileName;
    string destinationPath;
    int noOfChunks;
};

struct downloadStatus{
    string fileName;
    int noOfChunks;
    vector<bool> chunkStatus;
    int chunksDown;
};
unordered_map<string, downloadStatus*> ds;

struct Task{
    int fd;
    string ip;
    string port;
    int chunkno;
    string fileName;
    pthread_cond_t *condPtr;
};

queue<Task> taskQueue;

string charToString(char *charArr){
    int len = 0;
    string str = "";
    while(charArr[len] != '\0'){
        str += charArr[len];
        len++;
    }
    return str;
}


int getlen(char *str){
    int len = 0;
    while(str[len] != '\0')
        len++;
    return len;
}

string getHash(int fd){
    SHA_CTX sha1_context, chunk_context;
    SHA1_Init(&sha1_context);
    char file_buffer[CHUNK_SIZE];
    unsigned char chunk_digest[SHA_DIGEST_LENGTH];
    char chunkString[2*SHA_DIGEST_LENGTH + 1];
    size_t bytes_read;
    while((bytes_read = read(fd, file_buffer, CHUNK_SIZE)) > 0){
        file_buffer[bytes_read] = '\0';
        SHA1_Update(&sha1_context, file_buffer, bytes_read);
    }
    unsigned char sha1_hash[SHA_DIGEST_LENGTH];
    SHA1_Final(sha1_hash, &sha1_context);
    char sha1String[2 * SHA_DIGEST_LENGTH + 1];
    for (int i = 0; i < SHA_DIGEST_LENGTH; i++) {
        snprintf(&sha1String[i * 2], 3, "%02x", sha1_hash[i]);
    }
    sha1String[2 * SHA_DIGEST_LENGTH] = '\0';
    string hash = string(sha1String);
    return hash;
}

string getChunkHash(char* charArr, size_t len){
    SHA_CTX chunk_context;
    SHA1_Init(&chunk_context);
    SHA1_Update(&chunk_context, charArr, len);
    unsigned char chunk_hash[SHA_DIGEST_LENGTH];
    SHA1_Final(chunk_hash, &chunk_context);
    char hash[2*SHA_DIGEST_LENGTH + 1];
    for(int i=0;i<SHA_DIGEST_LENGTH;i++){
        snprintf(&hash[i*2], 3, "%02x", chunk_hash[i]);
    }
    hash[2*SHA_DIGEST_LENGTH] = '\0';
    string hashString = string(hash);
    return hashString;
}

int getChunk(int fd, string ip, string port, int chunkno, string filename){
    int psockfd = socket(AF_INET, SOCK_STREAM, 0);
    int pportno = stoi(port);
    struct sockaddr_in peer_addr;
    struct in_addr paddr;
    if(inet_pton(AF_INET, ip.c_str(), &paddr) <= 0){
        cout << "Invalid IP address for tracker" << endl;
        exit(1);
    }
    peer_addr.sin_family = AF_INET; 
    peer_addr.sin_port = htons(pportno); 
    peer_addr.sin_addr.s_addr = paddr.s_addr;
    if(connect(psockfd, (struct sockaddr*)&peer_addr, sizeof(peer_addr)) != 0){
        perror("connect error");
        exit(1);
    }
    char chunk_buff[CHUNK_SIZE + 1];
    write(psockfd, "get_chunk", 9);
    size_t bytes_read = read(psockfd, chunk_buff, 10);
    chunk_buff[bytes_read] = '\0';
    if(strcmp(chunk_buff, "1") == 0)
        return -1;



    string temp = to_string(chunkno) + ":" + filename;
    write(psockfd, temp.c_str(), temp.size());
    bytes_read = read(psockfd, chunk_buff, 100);
    chunk_buff[bytes_read] = '\0';



    string hashOfChunk = string(chunk_buff);
    write(psockfd, "OK", 2);
    size_t siz = read(psockfd, chunk_buff, CHUNK_SIZE);
    chunk_buff[siz] = '\0';

    
    string calHash = getChunkHash(chunk_buff, siz);
    cout << calHash << endl;
    if(calHash != hashOfChunk){
        cout << "bufferNotEqual: " << hashOfChunk << " " << calHash << " " << chunkno << endl;
    }
    else{
        cout << "buffer Equal: " << hashOfChunk << " " << calHash << " " << chunkno << endl;
    }
    close(psockfd);
    fdlock.lock();
    lseek(fd, ((chunkno - 1) * CHUNK_SIZE), SEEK_SET);
    write(fd, chunk_buff, siz);
    fdlock.unlock();
}

void notify_tracker(string file){
    int tsockfd, tportno;
    struct sockaddr_in serv_addr;
    struct in_addr taddr;
    tportno = stoi(gtrackerport);
    tsockfd = socket(AF_INET, SOCK_STREAM, 0);
    if(inet_pton(AF_INET, gtrackerip.c_str(), &taddr) <= 0){
        cout << "Invalid IP address for tracker" << endl;
        exit(1);
    }
    serv_addr.sin_family = AF_INET; 
    serv_addr.sin_port = htons(tportno); 
    serv_addr.sin_addr.s_addr = taddr.s_addr;
    if(connect(tsockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) != 0){
        perror("connect error");
        exit(1);
    }
    char buffer[1000];
    write(tsockfd, "notify", 6);
    size_t bytes_read = read(tsockfd, buffer, 1000);
    buffer[bytes_read] = '\0';
    if(strcmp(buffer, "0") != 0)
        return;
    string notifyMsg = "";
    notifyMsg = curr_user + " " + file;
    write(tsockfd, notifyMsg.c_str(), notifyMsg.size());
    close(tsockfd);
}

void *taskThread(void *){
    while(1){
        Task first;
        pthread_mutex_lock(&tqmutex);
        while(taskQueue.size() == 0){
            pthread_cond_wait(&condQueue, &tqmutex);
        }
        first = taskQueue.front();
        taskQueue.pop();
        pthread_mutex_unlock(&tqmutex);
        getChunk(first.fd, first.ip, first.port, first.chunkno, first.fileName);
        bool flag = false;
        int cd, cc;
        pthread_mutex_lock(&dsmutex);
        ds[first.fileName]->chunksDown++;
        cd = ds[first.fileName]->chunksDown;
        ds[first.fileName]->chunkStatus[first.chunkno] = true;
        cc = ds[first.fileName]->noOfChunks;
        if(cd == 1)
            flag = true;
        pthread_mutex_unlock(&dsmutex);
        cout << cc << " " << cd << endl;
        if(flag){
            notify_tracker(first.fileName);
        }
        if(cc == cd){
            pthread_cond_signal(first.condPtr);
        }
        shflock.lock();
        //check if shinfo[fileName] is NULL and only then update.
        shinfo[first.fileName]->chunks += (to_string(first.chunkno) + ";");
        shflock.unlock();
    }
}

void submitTask(Task t){
    pthread_mutex_lock(&tqmutex);
    taskQueue.push(t);
    pthread_mutex_unlock(&tqmutex);
    pthread_cond_broadcast(&condQueue);
}

void *downloadThread(void* args){
    downloadFileInfo *threadArgs = static_cast<downloadFileInfo*>(args);
    string peers = threadArgs->peers;
    string hash = threadArgs->hash;
    string fileName = threadArgs->fileName;
    string destPath = threadArgs->destinationPath;
    int n = threadArgs->noOfChunks;
    delete threadArgs;
    struct peerData{
        string ip;
        string port;
        string chunks;
    };
    vector<peerData> v;
    peerData place;
    char temp[peers.size() + 1];
    for(int i=0;i<peers.size();i++)
        temp[i] = peers[i];
    temp[peers.size()] = '\0';
    char *data = strtok(temp, ";");
    strtok(data, ":");
    place.ip = string(strtok(NULL, ":"));
    place.port = string(strtok(NULL, ":"));
    place.chunks = "";
    v.push_back(place);
    while((data = strtok(NULL, ";")) != NULL){
        strtok(data, ":");
        place.ip = string(strtok(NULL, ":"));
        place.port = string(strtok(NULL, ":"));
        place.chunks = "";
        v.push_back(place);
    }
    char buff[10000];
    for(auto &x:v){
        string curr_ip = x.ip;
        int port = stoi(x.port);
        struct sockaddr_in peer_addr;
        struct in_addr paddr;
        int tempsocket = socket(AF_INET, SOCK_STREAM, 0);
        inet_pton(AF_INET, curr_ip.c_str(), &paddr);
        peer_addr.sin_family = AF_INET;
        peer_addr.sin_addr.s_addr = paddr.s_addr;
        peer_addr.sin_port = htons(port);
        connect(tempsocket, (struct sockaddr*)&peer_addr, sizeof(peer_addr));
        string msg = "get_data ";
        msg = msg + fileName;
        write(tempsocket, msg.c_str(), msg.size());
        int bytes_read = read(tempsocket, buff, 10000);
        buff[bytes_read] = '\0';
        x.chunks = string(buff);
        close(tempsocket);
    }
    vector<vector<pair<string, string>>> pieces(n + 1);
    cout << v[0].chunks << endl;
    for(auto x:v){
        strcpy(buff, x.chunks.c_str());
        int num = atoi(strtok(buff, ";"));
        pieces[num].push_back({x.ip, x.port});
        char *temp1;
        while((temp1 = strtok(NULL, ";")) != NULL){
            num = atoi(temp1);
            pieces[num].push_back({x.ip, x.port});
        }
    }
    int fd = open(destPath.c_str(), O_CREAT | O_RDWR, 0666);
    srand(time(0));
    pthread_mutex_lock(&dsmutex);
    ds[fileName] = new downloadStatus;
    ds[fileName]->chunkStatus = vector<bool>(n+1, false);
    ds[fileName]->chunksDown = 0;
    ds[fileName]->noOfChunks = n;
    pthread_mutex_unlock(&dsmutex);
    shflock.lock();
    shinfo[fileName] = new SharedFiles;
    shinfo[fileName]->fileName = fileName;
    shinfo[fileName]->filePath = destPath;
    shinfo[fileName]->chunks = "";
    shflock.unlock();
    
    Task tasksFor;
    pthread_cond_t condComp;
    pthread_cond_init(&condComp, NULL);
    for(int i=1;i<=n;i++){
        int in = rand()%pieces[i].size();
        tasksFor.chunkno = i;
        tasksFor.ip = pieces[i][in].first;
        tasksFor.port = pieces[i][in].second;
        tasksFor.fd = fd;
        tasksFor.fileName = fileName;
        tasksFor.condPtr = &condComp;
        submitTask(tasksFor);
    }
    cout <<"UNtil here it is working" << endl;
    while(1){
        pthread_mutex_lock(&dsmutex);
        while(ds[fileName]->chunksDown != ds[fileName]->noOfChunks){
            pthread_cond_wait(&condComp, &dsmutex);
        }
        pthread_mutex_unlock(&dsmutex);
        break;
    }
    fdlock.lock();
    lseek(fd, 0, SEEK_SET);
    string compare_hash = getHash(fd);
    fdlock.unlock();
    if(compare_hash == hash)
        cout << fileName << ": Downloaded Successfully" << endl;
    else 
        cout << fileName << ": Unsuccessful Download" << endl;
    pthread_cond_destroy(&condComp);
    pthread_exit(NULL);
}

void *user(void* args){
    userThreadArgs *threadArgs = static_cast<userThreadArgs*>(args);
    int tsockfd, tportno;
    struct sockaddr_in serv_addr;
    struct in_addr taddr;
    bool loggedIn = false;
    tportno = stoi(threadArgs->tport);
    tsockfd = socket(AF_INET, SOCK_STREAM, 0);
    if(inet_pton(AF_INET, threadArgs->tip, &taddr) <= 0){
        cout << "Invalid IP address for tracker" << endl;
        exit(1);
    }
    serv_addr.sin_family = AF_INET; 
    serv_addr.sin_port = htons(tportno); 
    serv_addr.sin_addr.s_addr = taddr.s_addr;
    if(connect(tsockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) != 0){
        perror("connect error");
        exit(1);
    }
    gtrackerip = string(threadArgs->tip);
    gtrackerport = string(threadArgs->tport);
    char buff[1000];
    char input[1000];
    char dummy[1000];
    // ------------------------------------ Send ip and port-----------------------------------
    write(tsockfd, "hello", 5);
    int bytes_read = read(tsockfd, buff, 1000);
    buff[bytes_read] = '\0';
    if(strcmp(buff, "send_ip_port") != 0){
        cout << "error" << nl;
        exit(1);
    }
    string s = ":";
    s = threadArgs->cip + s;
    s += threadArgs->cport;
    write(tsockfd, s.c_str(), s.size());
    bytes_read = read(tsockfd, buff, 1000);
    buff[bytes_read] = '\0';
    if(strcmp(buff, "ok") != 0){
        cout << "error" << nl;
        exit(1);
    }
    string user = "";
    //-----------------------------------------------------------------------------------------

    //-------------------------------------------Executing Commands-------------------------------
    while(1){
        cin.getline(input, 1000);
        strcpy(dummy, input);
        char *cmd = strtok(dummy, " ");
        if(strcmp(cmd, "create_user") == 0)
        {
            char *username = strtok(NULL, " ");
            //handle error

            write(tsockfd, input, getlen(input));

            bytes_read = read(tsockfd, buff, 1000);
            buff[bytes_read] = '\0';
            if(strcmp(buff, "1") == 0){
                cout << "Username Already Exists" << nl;
                continue;
            }
            int status = mkdir(username, S_IRWXU | S_IRWXG | S_IRWXO);
            cout << "User Created Succesfully" << endl;
            //handle error

        }
        else if(strcmp(cmd, "login") == 0){
            if(loggedIn){
                cout << "Already Logged In" << endl;
                continue;
            }
            write(tsockfd, input, getlen(input));
            bytes_read = read(tsockfd, buff, 1000);
            buff[bytes_read] = '\0';
            if(strcmp(buff, "1") == 0){
                cout << "Username Does not exist" << endl;
                continue;
            }
            else if(strcmp(buff, "2") == 0){
                cout << "Password is wrong" << endl;
                continue;
            }
            else if(strcmp(buff, "3") == 0){
                cout << "User logged in from different system" << endl;
                continue;
            }
            loggedIn = true;
            user = string(strtok(NULL, " "));
            curr_user = user;
            cout << "Logged In" << endl;
        }
        else if(strcmp(cmd, "upload_file") == 0 && loggedIn){
            char *filePath = strtok(NULL, " ");
            string fileName = string(filePath);
            string path = "/";
            path = user + path;
            path = path + string(filePath);
            int fd = open(path.c_str(), O_RDONLY);
            if(fd < 0){
                perror("File error");
                continue;
            }
            write(tsockfd, input, getlen(input));
            bytes_read = read(tsockfd, buff, 1000);
            buff[bytes_read] = '\0';
            if(strcmp(buff, "1") == 0){
                cout << "File with this name is in sharing" << endl;
                cout << "Choose a different file name" << endl;
                continue;
            }
            string res = getHash(fd);
            string msg = res;
            size_t n = lseek(fd, 0, SEEK_END);
            close(fd);
            int noOfChunks = n / CHUNK_SIZE;
            if(n%CHUNK_SIZE != 0){
                noOfChunks += 1;
            }
            msg = msg + to_string(noOfChunks);
            write(tsockfd, msg.c_str(), msg.size());
            bytes_read = read(tsockfd, buff, 1000);
            buff[bytes_read] = '\0';
            if(strcmp(buff, "0") == 0){
                cout << "File Uploaded" << endl;
            }
            shflock.lock();
            shinfo[fileName] = new SharedFiles;
            string chunks = "";
            
            for(int i=1;i<=noOfChunks;i++){
                chunks += (to_string(i) + ";");
            }
            shinfo[fileName]->chunks = chunks;
            shinfo[fileName]->fileName = fileName;
            shinfo[fileName]->filePath = path;
            shflock.unlock();
        }
        else if(strcmp(cmd, "download_file") == 0 && loggedIn){
            write(tsockfd, input, getlen(input));
            bytes_read = read(tsockfd, buff, 1000);
            buff[bytes_read] = '\0';
            if(strcmp(buff, "1") == 0){
                cout << "There is no such file" << endl;
                continue;
            }
            else if(strcmp(buff, "2") == 0){
                cout << "Users are currently not online, try after some time" << endl;
                continue;
            }
            else{
                downloadFileInfo *df = new downloadFileInfo;
                df->fileName = string(strtok(NULL, " "));
                df->peers = string(strtok(buff, " "));
                write(tsockfd, "0", 1);
                bytes_read = read(tsockfd, buff, 1000);
                buff[bytes_read] = '\0';
                string temp = string(strtok(buff, " "));
                df->hash = temp.substr(0, 40);
                df->noOfChunks = stoi(temp.substr(40));
                df->destinationPath = user + "/" + df->fileName;
                pthread_t threadId;
                pthread_create(&threadId, NULL, downloadThread, static_cast<void*>(df));
                pthread_detach(threadId);
            }
        }
        else if(strcmp(cmd, "logout") == 0 && loggedIn){
            write(tsockfd, "logout", 6);
            loggedIn = false;
            user = "";
        }
    }
    //----------------------------------------------------------------------------------------------
    close(tsockfd);
    return NULL;
}

struct tds{
    int p;
    int con;
};

void *handle_request_peer(void *args){
    tds *hr = static_cast<tds*>(args);
    int psockfd = hr->p;
    int cone = hr->con;
    delete hr;
    char input_from_peer[1000];
    size_t bytes_read = read(psockfd, input_from_peer, 1000);
    input_from_peer[bytes_read] = '\0';
    char *temp = strtok(input_from_peer, " ");
    if(strcmp("get_chunk", input_from_peer) == 0){
        write(psockfd, "0", 1);
        bytes_read = read(psockfd, input_from_peer, 1000);
        input_from_peer[bytes_read] = '\0';
        int chunkNo = atoi(strtok(input_from_peer, ":"));
        string fn = string(strtok(NULL, ":"));
        shflock.lock();
        string fp = shinfo[fn]->filePath;
        shflock.unlock();
        int fd = open(fp.c_str(), O_RDONLY);
        char fileData[CHUNK_SIZE+1];
        fdlock.lock();
        lseek(fd, ((chunkNo-1)*CHUNK_SIZE), SEEK_SET);
        bytes_read = read(fd, fileData, CHUNK_SIZE);
        size_t siz = bytes_read;
        fileData[bytes_read] = '\0';
        fdlock.unlock();
        close(fd);
        string hashOfChunk = getChunkHash(fileData, siz);
        cout << hashOfChunk << endl;
        cout << siz << endl;
        write(psockfd, hashOfChunk.c_str(), hashOfChunk.size());
        bytes_read = read(psockfd, input_from_peer, 10);
        input_from_peer[bytes_read] = '\0';
        if(strcmp(input_from_peer, "OK") != 0){
            cout << "kartheek " << endl;
            close(psockfd);
            pthread_exit(NULL);
        }
        write(psockfd, fileData, siz);
    }
    else if(strcmp("get_data", input_from_peer) == 0){
        string fn = string(strtok(NULL, " "));
        string sendinfo = "";
        shflock.lock();
        sendinfo = shinfo[fn]->chunks;
        shflock.unlock();
        write(psockfd, sendinfo.c_str(), sendinfo.size());
    }
    else{
        cout << "some thing else: " << input_from_peer << endl;
    }
    close(psockfd);
    pthread_exit(NULL);
}



void *server(void *args){
    serverThreadArgs *threadArgs = static_cast<serverThreadArgs*>(args);
    socklen_t plen;
    int mysockfd, psockfd;
    int myportno = stoi(threadArgs->cport);
    struct sockaddr_in my_saddr,p_saddr;
    struct in_addr myip;
    mysockfd = socket(AF_INET, SOCK_STREAM, 0);
    if(inet_pton(AF_INET, threadArgs->cip, &myip) <= 0){
        cout << "Error: Invalid Ip address" << endl;
        exit(1);
    }
    my_saddr.sin_family = AF_INET;
    my_saddr.sin_addr.s_addr = myip.s_addr;
    my_saddr.sin_port = htons(myportno);
    int opt = 1;
    if (setsockopt(mysockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(int)) < 0) {
        perror("setsockopt");
        // Handle the error
    }
    if (bind(mysockfd, (struct sockaddr *) &my_saddr, sizeof(my_saddr)) < 0) {
        perror("bind error");
        exit(1);
    }
    listen(mysockfd, 5);
    // cout << "listening" << nl;
    int cone = 0;
    while((psockfd = accept(mysockfd, (struct sockaddr *) &p_saddr, &plen)) > 0){
        cone++;
        pthread_t con;
        tds *what = new tds;
        what->p = psockfd;
        what->con = cone;
        pthread_create(&con, NULL, handle_request_peer, static_cast<void*>(what));
        pthread_detach(con);
    }
    close(mysockfd);
    return NULL;
}

int main(int argc, char *argv[]){
    pthread_t userThread, serverThread;
    pthread_cond_init(&condQueue, NULL);
    pthread_mutex_init(&tqmutex, NULL);
    pthread_mutex_init(&dsmutex, NULL);
    if(argc != 3){
        cout << "Error: ./client <IP>:<PORT> tracker_info.txt" << nl;
        exit(1);
    }
    char *client_ip = strtok(argv[1], ":");
    char *client_port = strtok(NULL, ":");
    char *trackerPort;
    char *trackerIP;
    char *trackerNo;
    int fd = open(argv[2], O_RDONLY);
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
    sdata.cip = strtok(client_ip, "");
    sdata.cport = strtok(client_port, "");
    userThreadArgs udata;
    udata.cip = strtok(client_ip, "");
    udata.cport = strtok(client_port, "");
    udata.tip = strtok(trackerIP, "");
    udata.tport = strtok(trackerPort, "");
    pthread_t tthreads[NUM_THREADS];
    for(int i=0;i<NUM_THREADS;i++){
        pthread_create(&tthreads[i], NULL, taskThread, NULL);
    }
    pthread_create(&userThread, NULL, user, static_cast<void*>(&udata));
    pthread_create(&serverThread, NULL, server, static_cast<void*>(&sdata));
    pthread_join(serverThread, NULL);
    pthread_join(userThread, NULL);
    for(int i=0;i<NUM_THREADS;i++){
        pthread_join(tthreads[i], NULL);
    }
    pthread_mutex_destroy(&dsmutex);
    pthread_mutex_destroy(&tqmutex);
    pthread_cond_destroy(&condQueue);
}


