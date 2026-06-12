const express = require('express');
const http = require('http');
const socketIO = require('socket.io');
const { v4: uuidv4 } = require('uuid');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = socketIO(server, {
    cors: { origin: "*", methods: ["GET", "POST"] }
});

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json({ limit: '10mb' }));

const liveStreams = new Map();
const onlineUsers = new Map();
const friendRequests = new Map(); // {toUserId: [{fromId, fromName, fromPhoto, time}]}
const friendships = new Map(); // {userId: [friendIds]}

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));
app.get('/login', (req, res) => res.sendFile(path.join(__dirname, 'public', 'login.html')));
app.get('/profile', (req, res) => res.sendFile(path.join(__dirname, 'public', 'profile.html')));
app.get('/friends', (req, res) => res.sendFile(path.join(__dirname, 'public', 'friends.html')));
app.get('/go-live', (req, res) => res.sendFile(path.join(__dirname, 'public', 'go-live.html')));
app.get('/watch/:streamId', (req, res) => res.sendFile(path.join(__dirname, 'public', 'watch.html')));
app.get('/health', (req, res) => res.json({ status: 'ok' }));

app.get('/api/streams', (req, res) => {
    const streams = [];
    liveStreams.forEach((stream, id) => {
        streams.push({
            id: id,
            title: stream.title,
            streamerName: stream.streamerName,
            streamerPhoto: stream.streamerPhoto,
            viewers: stream.viewers,
            likes: stream.likes || 0,
            startedAt: stream.startedAt,
            category: stream.category,
            hashtags: stream.hashtags || []
        });
    });
    res.json(streams);
});

app.get('/api/online-users', (req, res) => {
    const users = [];
    onlineUsers.forEach((user, socketId) => {
        users.push({
            socketId: socketId,
            name: user.name,
            avatar: user.avatar,
            photo: user.photo,
            status: user.status || 'online',
            bio: user.bio || '',
            userEmail: user.userEmail
        });
    });
    res.json(users);
});

app.get('/api/trending', (req, res) => {
    const trending = [];
    const tagCount = {};
    
    liveStreams.forEach(stream => {
        if (stream.hashtags) {
            stream.hashtags.forEach(tag => {
                tagCount[tag] = (tagCount[tag] || 0) + stream.viewers + 1;
            });
        }
    });
    
    Object.entries(tagCount)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 10)
        .forEach(([tag, score]) => trending.push({ tag, score }));
    
    res.json(trending);
});

io.on('connection', (socket) => {

    socket.on('user-online', (data) => {
        onlineUsers.set(socket.id, {
            name: data.name,
            avatar: data.avatar || data.name.charAt(0).toUpperCase(),
            photo: data.photo || null,
            status: data.status || 'online',
            bio: data.bio || '',
            userEmail: data.userEmail || data.name
        });
        io.emit('users-updated');
        
        // Send pending friend requests
        const userId = data.userEmail || data.name;
        const requests = friendRequests.get(userId) || [];
        if (requests.length > 0) {
            socket.emit('pending-friend-requests', requests);
        }
    });

    socket.on('update-profile', (data) => {
        const user = onlineUsers.get(socket.id);
        if (user) {
            if (data.name) user.name = data.name;
            if (data.photo) user.photo = data.photo;
            if (data.bio !== undefined) user.bio = data.bio;
            if (data.status) user.status = data.status;
            io.emit('users-updated');
        }
    });

    // FRIEND SYSTEM
    socket.on('send-friend-request', (data) => {
        const sender = onlineUsers.get(socket.id);
        const recipient = io.sockets.sockets.get(data.toSocketId);
        const recipientData = onlineUsers.get(data.toSocketId);
        
        if (sender && recipient && recipientData) {
            const toUserId = recipientData.userEmail || recipientData.name;
            const fromUserId = sender.userEmail || sender.name;
            
            if (!friendRequests.has(toUserId)) {
                friendRequests.set(toUserId, []);
            }
            
            const existing = friendRequests.get(toUserId).find(r => r.fromUserId === fromUserId);
            if (existing) {
                socket.emit('friend-request-status', { message: 'Already sent!', status: 'error' });
                return;
            }
            
            const request = {
                fromSocketId: socket.id,
                fromUserId: fromUserId,
                fromName: sender.name,
                fromPhoto: sender.photo,
                fromAvatar: sender.avatar,
                time: new Date().toISOString()
            };
            
            friendRequests.get(toUserId).push(request);
            
            io.to(data.toSocketId).emit('new-friend-request', request);
            socket.emit('friend-request-status', { message: 'Friend request sent!', status: 'success' });
        }
    });

    socket.on('accept-friend-request', (data) => {
        const accepter = onlineUsers.get(socket.id);
        if (!accepter) return;
        
        const accepterId = accepter.userEmail || accepter.name;
        const requesterId = data.fromUserId;
        
        // Add to friendships
        if (!friendships.has(accepterId)) friendships.set(accepterId, []);
        if (!friendships.has(requesterId)) friendships.set(requesterId, []);
        
        if (!friendships.get(accepterId).includes(requesterId)) {
            friendships.get(accepterId).push(requesterId);
        }
        if (!friendships.get(requesterId).includes(accepterId)) {
            friendships.get(requesterId).push(accepterId);
        }
        
        // Remove from pending requests
        const requests = friendRequests.get(accepterId) || [];
        friendRequests.set(accepterId, requests.filter(r => r.fromUserId !== requesterId));
        
        // Notify both users
        socket.emit('friend-added', { 
            friendId: requesterId,
            friendName: data.fromName,
            friendPhoto: data.fromPhoto
        });
        
        // Notify the requester if online
        onlineUsers.forEach((user, sId) => {
            if ((user.userEmail || user.name) === requesterId) {
                io.to(sId).emit('friend-request-accepted', {
                    friendId: accepterId,
                    friendName: accepter.name,
                    friendPhoto: accepter.photo
                });
            }
        });
    });

    socket.on('decline-friend-request', (data) => {
        const accepter = onlineUsers.get(socket.id);
        if (!accepter) return;
        
        const accepterId = accepter.userEmail || accepter.name;
        const requests = friendRequests.get(accepterId) || [];
        friendRequests.set(accepterId, requests.filter(r => r.fromUserId !== data.fromUserId));
    });

    socket.on('get-friend-requests', () => {
        const user = onlineUsers.get(socket.id);
        if (!user) return;
        const userId = user.userEmail || user.name;
        const requests = friendRequests.get(userId) || [];
        socket.emit('pending-friend-requests', requests);
    });

    socket.on('remove-friend', (data) => {
        const remover = onlineUsers.get(socket.id);
        if (!remover) return;
        
        const removerId = remover.userEmail || remover.name;
        const friendId = data.friendId;
        
        if (friendships.has(removerId)) {
            friendships.set(removerId, friendships.get(removerId).filter(id => id !== friendId));
        }
        if (friendships.has(friendId)) {
            friendships.set(friendId, friendships.get(friendId).filter(id => id !== removerId));
        }
        
        socket.emit('friend-removed', { friendId });
    });

    socket.on('send-message', (data) => {
        const sender = onlineUsers.get(socket.id);
        const recipient = io.sockets.sockets.get(data.toSocketId);
        if (sender && recipient) {
            const timestamp = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
            io.to(data.toSocketId).emit('receive-message', {
                fromSocketId: socket.id,
                fromName: sender.name,
                fromAvatar: sender.avatar,
                fromPhoto: sender.photo,
                message: data.message,
                time: timestamp
            });
            socket.emit('message-sent', {
                toSocketId: data.toSocketId,
                message: data.message,
                time: timestamp
            });
        } else {
            socket.emit('message-error', { message: 'User is offline' });
        }
    });

    socket.on('typing', (data) => {
        const sender = onlineUsers.get(socket.id);
        if (sender) io.to(data.toSocketId).emit('user-typing', { fromSocketId: socket.id, fromName: sender.name });
    });

    socket.on('stop-typing', (data) => {
        io.to(data.toSocketId).emit('user-stop-typing', { fromSocketId: socket.id });
    });

    socket.on('start-stream', (data) => {
        const streamId = uuidv4().substring(0, 8);
        
        // Extract hashtags from title
        const hashtags = (data.title.match(/#\w+/g) || []).map(t => t.toLowerCase());
        
        liveStreams.set(streamId, {
            title: data.title || 'Untitled',
            streamerName: data.streamerName || 'Anonymous',
            streamerPhoto: data.streamerPhoto || null,
            streamerId: socket.id,
            viewers: 0,
            likes: 0,
            startedAt: new Date().toISOString(),
            category: data.category || 'Chat',
            hashtags: hashtags,
            peerId: data.peerId
        });
        socket.join(streamId);
        socket.streamId = streamId;
        socket.emit('stream-started', { streamId: streamId });
        io.emit('streams-updated');
    });

    socket.on('join-stream', (data) => {
        const stream = liveStreams.get(data.streamId);
        if (stream) {
            stream.viewers++;
            socket.join(data.streamId);
            socket.watchingStream = data.streamId;
            io.to(data.streamId).emit('viewer-count', { count: stream.viewers });
            socket.emit('streamer-info', {
                peerId: stream.peerId,
                title: stream.title,
                streamerName: stream.streamerName,
                streamerPhoto: stream.streamerPhoto,
                category: stream.category,
                likes: stream.likes,
                hashtags: stream.hashtags || []
            });
        } else {
            socket.emit('stream-error', { message: 'Stream not found' });
        }
    });

    socket.on('like-stream', (data) => {
        const stream = liveStreams.get(data.streamId);
        if (stream) {
            stream.likes++;
            io.to(data.streamId).emit('stream-liked', {
                likes: stream.likes,
                fromName: data.fromName
            });
        }
    });

    socket.on('send-reaction', (data) => {
        io.to(data.streamId).emit('new-reaction', {
            emoji: data.emoji,
            fromName: data.fromName
        });
    });

    socket.on('send-gift', (data) => {
        io.to(data.streamId).emit('new-gift', {
            gift: data.gift,
            fromName: data.fromName
        });
    });

    socket.on('chat-message', (data) => {
        io.to(data.streamId).emit('new-message', {
            name: data.name,
            photo: data.photo,
            message: data.message,
            time: new Date().toLocaleTimeString()
        });
    });

    socket.on('end-stream', () => {
        if (socket.streamId) {
            io.to(socket.streamId).emit('stream-ended', { message: 'Stream ended' });
            liveStreams.delete(socket.streamId);
            io.emit('streams-updated');
        }
    });

    socket.on('disconnect', () => {
        if (onlineUsers.has(socket.id)) {
            onlineUsers.delete(socket.id);
            io.emit('users-updated');
        }
        if (socket.streamId) {
            io.to(socket.streamId).emit('stream-ended', { message: 'Streamer disconnected' });
            liveStreams.delete(socket.streamId);
            io.emit('streams-updated');
        }
        if (socket.watchingStream) {
            const stream = liveStreams.get(socket.watchingStream);
            if (stream) {
                stream.viewers = Math.max(0, stream.viewers - 1);
                io.to(socket.watchingStream).emit('viewer-count', { count: stream.viewers });
            }
        }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log('F12 ORBIT v7.0 - Phase 2 - Port ' + PORT);
});