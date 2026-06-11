const express = require('express');
const http = require('http');
const socketIO = require('socket.io');
const { v4: uuidv4 } = require('uuid');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = socketIO(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

// Static files serve করবে
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

// সব live streams store করার জন্য (memory তে - server বন্ধ হলে মুছে যাবে)
const liveStreams = new Map();

// ==================== ROUTES ====================

// Home Page
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Go Live Page
app.get('/go-live', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'go-live.html'));
});

// Watch Stream Page
app.get('/watch/:streamId', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'watch.html'));
});

// Health check (Render এর জন্য)
app.get('/health', (req, res) => {
    res.json({ status: 'ok', uptime: process.uptime() });
});

// API: সব live streams দেখাও
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

// ==================== SOCKET.IO ====================

io.on('connection', (socket) => {
    console.log('✅ User connected:', socket.id);

    // Streamer live যাচ্ছে
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

        console.log(`🔴 Stream started: ${streamId} by ${data.streamerName}`);
        
        socket.emit('stream-started', { 
            streamId: streamId,
            message: 'You are now LIVE!' 
        });

        // সবাইকে জানাও নতুন stream এসেছে
        io.emit('streams-updated');
    });

    // Viewer stream দেখতে চায়
    socket.on('join-stream', (data) => {
        const stream = liveStreams.get(data.streamId);
        if (stream) {
            stream.viewers++;
            socket.join(data.streamId);
            socket.watchingStream = data.streamId;

            // Streamer কে বলো নতুন viewer এসেছে
            io.to(data.streamId).emit('viewer-count', { 
                count: stream.viewers 
            });

            // Viewer কে streamer এর peer ID দাও
            socket.emit('streamer-peer-id', { 
                peerId: stream.peerId 
            });

            console.log(`👁 Viewer joined stream: ${data.streamId}, Total: ${stream.viewers}`);
        } else {
            socket.emit('stream-error', { 
                message: 'This stream has ended or does not exist.' 
            });
        }
    });

    // Live Chat
    socket.on('chat-message', (data) => {
        const streamId = data.streamId;
        io.to(streamId).emit('new-message', {
            name: data.name,
            message: data.message,
            time: new Date().toLocaleTimeString()
        });
    });

    // Stream end করো
    socket.on('end-stream', () => {
        if (socket.streamId) {
            const streamId = socket.streamId;
            
            // সব viewers কে জানাও stream শেষ
            io.to(streamId).emit('stream-ended', {
                message: 'The stream has ended.'
            });

            // Stream delete করো (VIDEO SAVE হবে না!)
            liveStreams.delete(streamId);
            
            console.log(`⬛ Stream ended: ${streamId}`);
            io.emit('streams-updated');
        }
    });

    // User disconnect হলে
    socket.on('disconnect', () => {
        console.log('❌ User disconnected:', socket.id);

        // যদি streamer disconnect হয়
        if (socket.streamId) {
            const streamId = socket.streamId;
            
            io.to(streamId).emit('stream-ended', {
                message: 'The streamer has disconnected.'
            });

            liveStreams.delete(streamId);
            io.emit('streams-updated');
        }

        // যদি viewer disconnect হয়
        if (socket.watchingStream) {
            const stream = liveStreams.get(socket.watchingStream);
            if (stream) {
                stream.viewers = Math.max(0, stream.viewers - 1);
                io.to(socket.watchingStream).emit('viewer-count', {
                    count: stream.viewers
                });
            }
        }
    });
});

// ==================== SERVER START ====================

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`
    ╔══════════════════════════════════════╗
    ║   🎬 Holo Gia Live Stream App       ║
    ║   🌐 Port: ${PORT}                      ║
    ║   ✅ Server is running!              ║
    ╚══════════════════════════════════════╝
    `);
});