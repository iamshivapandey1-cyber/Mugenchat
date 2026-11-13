MUGENCHAT
Run:  node server.js   then open http://localhost:3000
Phone app: deploy on HTTPS (Render.com), open in Chrome > menu > "Install app".
Voice notes and calls need HTTPS or localhost.

KEEP DATA SAFE: set env DATA_DIR to a folder on a persistent disk (e.g. /data on Render
with a Persistent Disk). Without it, free hosts wipe data.json and uploads on restart.
Login stays on the device until you press Log out.
