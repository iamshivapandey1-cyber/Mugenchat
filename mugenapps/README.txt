MUGENCHAT v2  (Node.js + MongoDB Atlas)
=======================================

RUN ON YOUR PC
  1. npm install
  2. node server.js        -> http://localhost:3000
  (Without MONGODB_URI the data is saved in data.json, same as before.)

MONGODB ATLAS (permanent database)
  1. cloud.mongodb.com -> create free cluster (M0).
  2. Database Access -> Add user (username + password).
  3. Network Access -> Add IP -> 0.0.0.0/0  (needed for Render/Railway).
  4. Connect -> Drivers -> copy the string:
       mongodb+srv://USER:PASSWORD@cluster0.xxxxx.mongodb.net/?retryWrites=true&w=majority
     (if the password has special characters like @ : / # encode them, e.g. @ = %40)
  5. Set environment variable  MONGODB_URI  = that string
     (optional: MONGODB_DB = mugenchat)
     PC:      set MONGODB_URI=...   (Windows)   |   export MONGODB_URI=...  (Mac/Linux)
     Render:  Environment tab -> Add variable.  Build command: npm install   Start: node server.js
  - Users, messages, groups, sessions AND photos/videos/voice notes are stored in Atlas,
    so nothing is lost when the host restarts.
  - First start with an empty database automatically imports your old data.json.
  - Free M0 gives 512 MB. Big videos fill it quickly.

INVITE LINKS:  https://YOUR-SITE/?join=CODE   (group admin creates it in Group info)

LIMITS: group call = up to 6 people (mesh WebRTC), group = 30 members.
Calls need HTTPS (or localhost). For calls across different networks add a TURN server.
