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
app.use(express.json());

const liveStreams = new Map();
const onlineUsers = new Map();

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.get('/login', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'login.html'));
});

app.get('/messages', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'messages.html'));
});

app.get('/go-live', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'go-live.html'));
});

app.get('/watch/:streamId', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'watch.html'));
});

app.get('/health', (req, res) => {
    res.json({ status: 'ok', uptime: process.uptime() });
});

app.get('/api/streams', (req, res) => {
    const streams = [];
    liveStreams.forEach((stream, id) => {
        streams.push({
            id: id,
            title: stream.title,
            streamerName: stream.streamerName,
            viewers: stream.viewers,
            startedAt: stream.startedAt
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
            status: user.status
        });
    });
    res.json(users);
});

io.on('connection', (socket) => {
    console.log('Connected:', socket.id);

    socket.on('user-online', (data) => {
        onlineUsers.set(socket.id, {
            name: data.name,
            avatar: data.avatar || data.name.charAt(0).toUpperCase(),
            status: 'online'
        });
        console.log(data.name + ' is online');
        io.emit('users-updated');
    });

    socket.on('send-message', (data) => {
        const recipient = data.toSocketId;
        const sender = onlineUsers.get(socket.id);
        
        if (sender && io.sockets.sockets.get(recipient)) {
            io.to(recipient).emit('receive-message', {
                fromSocketId: socket.id,
                fromName: sender.name,
                fromAvatar: sender.avatar,
                message: data.message,
                time: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
            });
            
            socket.emit('message-sent', {
                toSocketId: recipient,
                message: data.message,
                time: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
            });
        }
    });

    socket.on('typing', (data) => {
        const sender = onlineUsers.get(socket.id);
        if (sender) {
            io.to(data.toSocketId).emit('user-typing', {
                fromSocketId: socket.id,
                fromName: sender.name
            });
        }
    });

    socket.on('stop-typing', (data) => {
        io.to(data.toSocketId).emit('user-stop-typing', {
            fromSocketId: socket.id
        });
    });

    socket.on('start-stream', (data) => {
        const streamId = uuidv4().substring(0, 8);
        
        liveStreams.set(streamId, {
            title: data.title || 'Untitled Stream',
            streamerName: data.streamerName || 'Anonymous',
            streamerId: socket.id,
            viewers: 0,
            startedAt: new Date().toISOString(),
            peerId: data.peerId
        });

        socket.join(streamId);
        socket.streamId = streamId;

        console.log('Stream started:', streamId);
        
        socket.emit('stream-started', { 
            streamId: streamId,
            message: 'You are now LIVE!' 
        });

        io.emit('streams-updated');
    });

    socket.on('join-stream', (data) => {
        const stream = liveStreams.get(data.streamId);
        if (stream) {
            stream.viewers++;
            socket.join(data.streamId);
            socket.watchingStream = data.streamId;

            io.to(data.streamId).emit('viewer-count', { count: stream.viewers });
            socket.emit('streamer-peer-id', { peerId: stream.peerId });
        } else {
            socket.emit('stream-error', { 
                message: 'Stream not found or ended.' 
            });
        }
    });

    socket.on('chat-message', (data) => {
        io.to(data.streamId).emit('new-message', {
            name: data.name,
            message: data.message,
            time: new Date().toLocaleTimeString()
        });
    });

    socket.on('end-stream', () => {
        if (socket.streamId) {
            const streamId = socket.streamId;
            io.to(streamId).emit('stream-ended', { message: 'Stream ended.' });
            liveStreams.delete(streamId);
            io.emit('streams-updated');
        }
    });

    socket.on('disconnect', () => {
        console.log('Disconnected:', socket.id);

        if (onlineUsers.has(socket.id)) {
            onlineUsers.delete(socket.id);
            io.emit('users-updated');
        }

        if (socket.streamId) {
            const streamId = socket.streamId;
            io.to(streamId).emit('stream-ended', { message: 'Streamer disconnected.' });
            liveStreams.delete(streamId);
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
    console.log('F12 ORBIT - Server running on port ' + PORT);
});