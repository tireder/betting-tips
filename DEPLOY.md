# 🚀 Deploy to Render.com - Quick Guide

## Step 1: Prepare Your Repository

1. **Commit all files** including:
   - `Dockerfile`
   - `.dockerignore`
   - `render.yaml` (optional)
   - All Python files
   - `requirements.txt`

2. **Push to GitHub/GitLab**

## Step 2: Create Render Service

1. Go to [Render Dashboard](https://dashboard.render.com)
2. Click **"New +"** → **"Web Service"**
3. Connect your repository
4. Configure:
   - **Name**: `betting-panel` (or your choice)
   - **Environment**: **Docker**
   - **Dockerfile Path**: `./Dockerfile`
   - **Docker Context**: `.` (root directory)

## Step 3: Set Environment Variables

In Render dashboard, add these **Environment Variables**:

| Key | Value | Required |
|-----|-------|----------|
| `OPENAI_API_KEY` | Your OpenAI API key | ✅ Yes |
| `API_FOOTBALL_KEY` | Your API-Football V3 key | ✅ Yes |
| `PORT` | `8501` | ⚠️ Auto-set by Render |

## Step 4: Deploy

1. Click **"Create Web Service"**
2. Render will:
   - Build the Docker image
   - Start the container
   - Provide a URL (e.g., `https://betting-panel.onrender.com`)

## Step 5: Verify

1. Wait for build to complete (~5-10 minutes first time)
2. Click the provided URL
3. You should see the Streamlit app!

## 🔧 Troubleshooting

### Build Fails
- Check build logs in Render dashboard
- Verify `requirements.txt` is valid
- Ensure all Python files are in repo

### App Won't Start
- Check runtime logs
- Verify environment variables are set
- Ensure API keys are valid

### Port Issues
- Render automatically sets `PORT` env var
- Dockerfile handles this via startup script
- If issues persist, check Render logs

## 📝 Notes

- **Free tier**: App sleeps after 15 min inactivity
- **Starter tier**: Always-on, better performance
- **Database**: SQLite cache is ephemeral (resets on restart)
- **Persistent storage**: Upgrade to use Render Disk

## 🔐 Security

✅ **DO:**
- Use Render's environment variable secrets
- Enable HTTPS (automatic on Render)
- Keep API keys secret

❌ **DON'T:**
- Commit API keys to git
- Share your Render dashboard access
- Expose internal ports

---

**Need help?** Check `README_DOCKER.md` for detailed information.

