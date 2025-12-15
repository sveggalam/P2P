1. Tracker(tracker.cpp)
The tracker is a centralized entity that keeps track of all the peers (clients) in the network. It maintains information about:

Files available for sharing
The list of peers holding specific pieces of the file
Managing client requests to join or leave the file-sharing network
The tracker is responsible for:

Receiving registration and file-sharing requests from clients.
Maintaining metadata (such as file hashes) to verify the integrity of the files.
Directing clients to other peers for file downloading or uploading.
2. Client (client.cpp)
The client allows users to share or download files from other peers in the network. Each client registers with the tracker and communicates with other clients to exchange files. The key functionalities include:

Upload: Sharing a file by breaking it into chunks and informing the tracker.
Download: Retrieving chunks of a file from multiple peers and assembling the file locally.
Peer Discovery: Obtaining information from the tracker about where to find specific file chunks.
How It Works
Client Registration: When a client joins the network, it connects to the tracker and registers its available files for sharing.

File Sharing: When a file is uploaded by a client, it informs the tracker about the file, including details like file name, size, and hash for verification. The tracker records which clients have the file.

File Downloading: When a client wants to download a file, it queries the tracker to get a list of peers that have the requested file or its chunks. The client can then download the file in parts (chunks) from multiple peers, ensuring efficiency and fault tolerance.

Chunking: Large files are divided into smaller pieces (chunks) for easier sharing and downloading. This allows parallel downloads from multiple peers, speeding up the process.

Features
Peer-to-Peer File Sharing: Files can be shared between multiple clients without relying on a single server.
Fault Tolerance: Files are downloaded in chunks from multiple peers, allowing downloads to continue even if some peers go offline.
Tracker Management: Centralized tracker to keep metadata and direct peers to each other for file sharing.
Secure Transfers: Uses SHA1 hashing for file chunks to ensure data integrity.
Compiling and Executing
Compile
Compile the tracker: g++ tracker.cpp -o tracker
Compile the client: g++ client.cpp -o client
Usage
Start the Tracker: Run the tracker to handle client registrations and file metadata. ./tracker <tracker_port>

Start the Client: Each client connects to the tracker and can either upload or download files. ./client <tracker_ip> <tracker_port>

Commands
upload <file_path>: Shares a file with the network.
download <file_name>: Downloads a file from peers.
exit: Disconnects the client from the network.
Future Improvements
Implementing encryption for secure file transfers.
Adding support for handling multiple trackers for better load distribution.
Improving peer discovery to avoid relying on a single tracker.
