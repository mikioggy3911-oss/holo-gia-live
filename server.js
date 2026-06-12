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
const friendRequests = new Map();
const friendships = new Map();
const scheduledStreams = new Map();
const userReports = new Map();
const blockedUsers = new Map();
const streamPolls = new Map();
const userProfiles = new Map();

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));
app.get('/login', (req, res) => res.sendFile(path.join(__dirname, 'public', 'login.html')));
app.get('/profile', (req, res) => res.sendFile(path.join(__dirname, 'public', 'profile.html')));
app.get('/friends', (req, res) => res.sendFile(path.join(__dirname, 'public', 'friends.html')));
app.get('/user/:userId', (req, res) => res.sendFile(path.join(__dirname, 'public', 'user-profile.html')));
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
            hashtags: stream.hashtags || [],
            streamerLevel: stream.streamerLevel || 1
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
            userEmail: user.userEmail,
            level: user.level || 1
        });
    });
    res.json(users);
});

app.get('/api/scheduled', (req, res) => {
    const scheduled = [];
    scheduledStreams.forEach((s, id) => {
        scheduled.push({ id, ...s });
    });
    res.json(scheduled.sort((a, b) => new Date(a.scheduledTime) - new Date(b.scheduledTime)));
});

app.get('/api/trending', (req, res) => {
    const tagCount = {};
    liveStreams.forEach(stream => {
        if (stream.hashtags) {
            stream.hashtags.forEach(tag => {
                tagCount[tag] = (tagCount[tag] || 0) + stream.viewers + 1;
            });
        }
    });
    const trending = Object.entries(tagCount).sort((a, b) => b[1] - a[1]).slice(0, 10).map(([tag, score]) => ({ tag, score }));
    res.json(trending);
});

app.get('/api/user/:userId', (req, res) => {
    const userId = req.params.userId;
    const profile = userProfiles.get(userId) || {};
    let socketId = null;
    let online = false;
    onlineUsers.forEach((u, sId) => {
        if ((u.userEmail || u.name) === userId) {
            socketId = sId;
            online = true;
        }
    });
    res.json({
        userId,
        socketId,
        online,
        ...profile
    });
});

io.on('connection', (socket) => {

    socket.on('user-online', (data) => {
        const userId = data.userEmail || data.name;
        
        // Calculate level based on activity
        const profile = userProfiles.get(userId) || { streams: 0, totalViews: 0, joinedAt: new Date().toISOString() };
        const level = calculateLevel(profile.streams || 0, profile.totalViews || 0);
        
        onlineUsers.set(socket.id, {
            name: data.name,
            avatar: data.avatar || data.name.charAt(0).toUpperCase(),
            photo: data.photo || null,
            status: data.status || 'online',
            bio: data.bio || '',
            userEmail: userId,
            level: level
        });
        
        // Update user profile
        userProfiles.set(userId, {
            ...profile,
            name: data.name,
            photo: data.photo,
            bio: data.bio,
            level: level,
            lastSeen: new Date().toISOString()
        });
        
        io.emit('users-updated');
        const requests = friendRequests.get(userId) || [];
        if (requests.length > 0) socket.emit('pending-friend-requests', requests);
    });

    function calculateLevel(streams, views) {
        const points = (streams * 10) + (views * 0.1);
        if (points >= 1000) return 5;
        if (points >= 500) return 4;
        if (points >= 200) return 3;
        if (points >= 50) return 2;
        return 1;
    }

    socket.on('update-profile', (data) => {
        const user = onlineUsers.get(socket.id);
        if (user) {
            if (data.name) user.name = data.name;
            if (data.photo) user.photo = data.photo;
            if (data.bio !== undefined) user.bio = data.bio;
            if (data.status) user.status = data.status;
            
            // Update stored profile
            const userId = user.userEmail || user.name;
            const profile = userProfiles.get(userId) || {};
            userProfiles.set(userId, { ...profile, ...data });
            
            io.emit('users-updated');
        }
    });

    // FRIEND SYSTEM
    socket.on('send-friend-request', (data) => {
        const sender = onlineUsers.get(socket.id);
        const recipientData = onlineUsers.get(data.toSocketId);
        if (sender && recipientData) {
            const toUserId = recipientData.userEmail || recipientData.name;
            const fromUserId = sender.userEmail || sender.name;
            if (!friendRequests.has(toUserId)) friendRequests.set(toUserId, []);
            const existing = friendRequests.get(toUserId).find(r => r.fromUserId === fromUserId);
            if (existing) { socket.emit('friend-request-status', { message: 'Already sent!', status: 'error' }); return; }
            const request = {
                fromSocketId: socket.id, fromUserId, fromName: sender.name,
                fromPhoto: sender.photo, fromAvatar: sender.avatar,
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
        if (!friendships.has(accepterId)) friendships.set(accepterId, []);
        if (!friendships.has(requesterId)) friendships.set(requesterId, []);
        if (!friendships.get(accepterId).includes(requesterId)) friendships.get(accepterId).push(requesterId);
        if (!friendships.get(requesterId).includes(accepterId)) friendships.get(requesterId).push(accepterId);
        const requests = friendRequests.get(accepterId) || [];
        friendRequests.set(accepterId, requests.filter(r => r.fromUserId !== requesterId));
        socket.emit('friend-added', { friendId: requesterId, friendName: data.fromName, friendPhoto: data.fromPhoto });
        onlineUsers.forEach((user, sId) => {
            if ((user.userEmail || user.name) === requesterId) {
                io.to(sId).emit('friend-request-accepted', { friendId: accepterId, friendName: accepter.name, friendPhoto: accepter.photo });
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
        if (friendships.has(removerId)) friendships.set(removerId, friendships.get(removerId).filter(id => id !== data.friendId));
        if (friendships.has(data.friendId)) friendships.set(data.friendId, friendships.get(data.friendId).filter(id => id !== removerId));
        socket.emit('friend-removed', { friendId: data.friendId });
    });

    // BLOCK & REPORT
    socket.on('block-user', (data) => {
        const blocker = onlineUsers.get(socket.id);
        if (!blocker) return;
        const blockerId = blocker.userEmail || blocker.name;
        if (!blockedUsers.has(blockerId)) blockedUsers.set(blockerId, []);
        if (!blockedUsers.get(blockerId).includes(data.userId)) {
            blockedUsers.get(blockerId).push(data.userId);
        }
        socket.emit('user-blocked', { userId: data.userId });
    });

    socket.on('unblock-user', (data) => {
        const blocker = onlineUsers.get(socket.id);
        if (!blocker) return;
        const blockerId = blocker.userEmail || blocker.name;
        if (blockedUsers.has(blockerId)) {
            blockedUsers.set(blockerId, blockedUsers.get(blockerId).filter(id => id !== data.userId));
        }
        socket.emit('user-unblocked', { userId: data.userId });
    });

    socket.on('report-user', (data) => {
        const reporter = onlineUsers.get(socket.id);
        if (!reporter) return;
        if (!userReports.has(data.userId)) userReports.set(data.userId, []);
        userReports.get(data.userId).push({
            reportedBy: reporter.userEmail || reporter.name,
            reason: data.reason,
            time: new Date().toISOString()
        });
        socket.emit('report-submitted', { message: 'Report submitted successfully' });
    });

    // SCHEDULE STREAMS
    socket.on('schedule-stream', (data) => {
        const user = onlineUsers.get(socket.id);
        if (!user) return;
        const id = uuidv4().substring(0, 8);
        scheduledStreams.set(id, {
            id,
            title: data.title,
            description: data.description || '',
            category: data.category,
            scheduledTime: data.scheduledTime,
            streamerName: user.name,
            streamerPhoto: user.photo,
            streamerId: user.userEmail || user.name,
            createdAt: new Date().toISOString(),
            interested: []
        });
        socket.emit('stream-scheduled', { id });
        io.emit('scheduled-updated');
    });

    socket.on('cancel-scheduled', (data) => {
        const user = onlineUsers.get(socket.id);
        if (!user) return;
        const userId = user.userEmail || user.name;
        const scheduled = scheduledStreams.get(data.id);
        if (scheduled && scheduled.streamerId === userId) {
            scheduledStreams.delete(data.id);
            io.emit('scheduled-updated');
        }
    });

    socket.on('interested-in-stream', (data) => {
        const user = onlineUsers.get(socket.id);
        if (!user) return;
        const userId = user.userEmail || user.name;
        const scheduled = scheduledStreams.get(data.id);
        if (scheduled) {
            if (!scheduled.interested.includes(userId)) {
                scheduled.interested.push(userId);
            } else {
                scheduled.interested = scheduled.interested.filter(u => u !== userId);
            }
            io.emit('scheduled-updated');
        }
    });

    // POLLS
    socket.on('create-poll', (data) => {
        const pollId = uuidv4().substring(0, 8);
        const poll = {
            id: pollId,
            question: data.question,
            options: data.options.map(opt => ({ text: opt, votes: 0, voters: [] })),
            createdAt: new Date().toISOString(),
            streamId: data.streamId
        };
        streamPolls.set(data.streamId, poll);
        io.to(data.streamId).emit('new-poll', poll);
    });

    socket.on('vote-poll', (data) => {
        const user = onlineUsers.get(socket.id);
        if (!user) return;
        const userId = user.userEmail || user.name;
        const poll = streamPolls.get(data.streamId);
        if (poll && !poll.options.some(opt => opt.voters.includes(userId))) {
            poll.options[data.optionIndex].votes++;
            poll.options[data.optionIndex].voters.push(userId);
            io.to(data.streamId).emit('poll-updated', poll);
        }
    });

    socket.on('close-poll', (data) => {
        const poll = streamPolls.get(data.streamId);
        if (poll) {
            io.to(data.streamId).emit('poll-closed', poll);
            streamPolls.delete(data.streamId);
        }
    });

    // SPIN THE WHEEL
    socket.on('spin-wheel', (data) => {
        const stream = liveStreams.get(data.streamId);
        if (!stream) return;
        const room = io.sockets.adapter.rooms.get(data.streamId);
        if (room) {
            const viewerIds = Array.from(room).filter(id => id !== stream.streamerId);
            if (viewerIds.length > 0) {
                const winnerSocketId = viewerIds[Math.floor(Math.random() * viewerIds.length)];
                const winner = onlineUsers.get(winnerSocketId);
                if (winner) {
                    io.to(data.streamId).emit('wheel-winner', {
                        winnerName: winner.name,
                        winnerPhoto: winner.photo,
                        prize: data.prize || 'a Special Mention!'
                    });
                }
            }
        }
    });

    // MUTE USER IN CHAT
    socket.on('mute-user-chat', (data) => {
        const muter = onlineUsers.get(socket.id);
        if (!muter) return;
        const muterId = muter.userEmail || muter.name;
        if (!blockedUsers.has(muterId + '_chat')) blockedUsers.set(muterId + '_chat', []);
        if (!blockedUsers.get(muterId + '_chat').includes(data.userId)) {
            blockedUsers.get(muterId + '_chat').push(data.userId);
        }
        socket.emit('chat-user-muted', { userId: data.userId });
    });

    // MESSAGES
    socket.on('send-message', (data) => {
        const sender = onlineUsers.get(socket.id);
        const recipient = io.sockets.sockets.get(data.toSocketId);
        if (sender && recipient) {
            const senderId = sender.userEmail || sender.name;
            const recipientData = onlineUsers.get(data.toSocketId);
            const recipientId = recipientData ? (recipientData.userEmail || recipientData.name) : null;
            
            // Check if blocked
            const recipientBlocked = blockedUsers.get(recipientId) || [];
            if (recipientBlocked.includes(senderId)) {
                socket.emit('message-error', { message: 'Cannot message this user' });
                return;
            }
            
            const timestamp = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
            io.to(data.toSocketId).emit('receive-message', {
                fromSocketId: socket.id, fromName: sender.name, fromAvatar: sender.avatar,
                fromPhoto: sender.photo, fromUserId: senderId, message: data.message, time: timestamp
            });
            socket.emit('message-sent', { toSocketId: data.toSocketId, message: data.message, time: timestamp });
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

    // STREAMS
    socket.on('start-stream', (data) => {
        const user = onlineUsers.get(socket.id);
        const userId = user ? (user.userEmail || user.name) : 'anonymous';
        
        // Update stream count
        const profile = userProfiles.get(userId) || {};
        profile.streams = (profile.streams || 0) + 1;
        userProfiles.set(userId, profile);
        
        const streamId = uuidv4().substring(0, 8);
        const hashtags = (data.title.match(/#\w+/g) || []).map(t => t.toLowerCase());
        
        liveStreams.set(streamId, {
            title: data.title || 'Untitled',
            streamerName: data.streamerName || 'Anonymous',
            streamerPhoto: data.streamerPhoto || null,
            streamerId: socket.id,
            streamerUserId: userId,
            streamerLevel: user ? user.level : 1,
            viewers: 0,
            likes: 0,
            reactions: { '😍': 0, '😂': 0, '🔥': 0, '👏': 0, '💯': 0 },
            startedAt: new Date().toISOString(),
            category: data.category || 'Chat',
            hashtags: hashtags,
            peerId: data.peerId,
            chatHistory: []
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
                streamerUserId: stream.streamerUserId,
                streamerLevel: stream.streamerLevel,
                category: stream.category,
                likes: stream.likes,
                hashtags: stream.hashtags || [],
                reactions: stream.reactions
            });
            
            // Send active poll if any
            const poll = streamPolls.get(data.streamId);
            if (poll) socket.emit('new-poll', poll);
        } else {
            socket.emit('stream-error', { message: 'Stream not found' });
        }
    });

    socket.on('like-stream', (data) => {
        const stream = liveStreams.get(data.streamId);
        if (stream) {
            stream.likes++;
            
            // Update streamer's total views/likes
            if (stream.streamerUserId) {
                const profile = userProfiles.get(stream.streamerUserId) || {};
                profile.totalViews = (profile.totalViews || 0) + 1;
                userProfiles.set(stream.streamerUserId, profile);
            }
            
            io.to(data.streamId).emit('stream-liked', { likes: stream.likes, fromName: data.fromName });
        }
    });

    socket.on('send-reaction', (data) => {
        const stream = liveStreams.get(data.streamId);
        if (stream) {
            if (stream.reactions[data.emoji] !== undefined) {
                stream.reactions[data.emoji]++;
            }
            io.to(data.streamId).emit('new-reaction', { emoji: data.emoji, fromName: data.fromName, totals: stream.reactions });
        }
    });

    socket.on('send-gift', (data) => {
        io.to(data.streamId).emit('new-gift', { gift: data.gift, fromName: data.fromName });
    });

    socket.on('chat-message', (data) => {
        const sender = onlineUsers.get(socket.id);
        if (!sender) return;
        const senderId = sender.userEmail || sender.name;
        
        const stream = liveStreams.get(data.streamId);
        if (!stream) return;
        
        // Check if user is muted by streamer
        const streamerMuted = blockedUsers.get(stream.streamerUserId + '_chat') || [];
        if (streamerMuted.includes(senderId)) {
            socket.emit('chat-muted', { message: 'You are muted in this stream' });
            return;
        }
        
        const message = {
            name: data.name,
            photo: data.photo,
            userId: senderId,
            message: data.message,
            time: new Date().toLocaleTimeString()
        };
        
        stream.chatHistory.push(message);
        if (stream.chatHistory.length > 100) stream.chatHistory.shift();
        
        io.to(data.streamId).emit('new-message', message);
    });

    socket.on('end-stream', () => {
        if (socket.streamId) {
            io.to(socket.streamId).emit('stream-ended', { message: 'Stream ended' });
            liveStreams.delete(socket.streamId);
            streamPolls.delete(socket.streamId);
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
            streamPolls.delete(socket.streamId);
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
    console.log('F12 ORBIT v8.0 - Phase 3 - Port ' + PORT);
});