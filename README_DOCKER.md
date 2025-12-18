# Docker Deployment for Render.com

This guide explains how to deploy the Betting Panel application to Render.com using Docker.

## 🚀 Quick Start

1. **Push your code to GitHub/GitLab**
2. **Connect to Render.com**
3. **Create a new Web Service**
4. **Select "Docker" as the environment**
5. **Set the Dockerfile path**: `./Dockerfile`
6. **Add environment variables** (see below)

## 📋 Environment Variables

Set these in the Render.com dashboard:

- `OPENAI_API_KEY` - Your OpenAI API key (for AI analysis)
- `API_FOOTBALL_KEY` - Your API-Football V3 key (for live data)

Optional:
- `STREAMLIT_SERVER_PORT` - Default: 8501
- `STREAMLIT_SERVER_ADDRESS` - Default: 0.0.0.0

## 🐳 Docker Build

To test locally:

```bash
# Build the image
docker build -t betting-panel .

# Run locally
docker run -p 8501:8501 \
  -e OPENAI_API_KEY=your_key \
  -e API_FOOTBALL_KEY=your_key \
  betting-panel
```

## 📝 Render.com Configuration

### Option 1: Using render.yaml (Recommended)

1. Push `render.yaml` to your repo
2. In Render dashboard, select "Apply render.yaml"
3. Render will automatically configure the service

### Option 2: Manual Configuration

1. **Service Type**: Web Service
2. **Environment**: Docker
3. **Dockerfile Path**: `./Dockerfile`
4. **Docker Context**: `.` (root)
5. **Build Command**: (auto-detected)
6. **Start Command**: (auto-detected from Dockerfile)

## 🔧 Troubleshooting

### Port Issues
- Render.com may assign a different PORT env var
- The Dockerfile uses port 8501 (Streamlit default)
- If Render uses a different port, update the Dockerfile CMD

### Build Failures
- Check that all dependencies in `requirements.txt` are valid
- Ensure Python version matches (3.11 in Dockerfile)

### Runtime Errors
- Check logs in Render dashboard
- Verify environment variables are set correctly
- Ensure API keys have proper permissions

## 📊 Health Check

The Dockerfile includes a health check endpoint:
- URL: `/_stcore/health`
- Interval: 30 seconds
- Timeout: 10 seconds

## 💾 Persistent Storage

The SQLite cache (`team_history_cache.db`) is stored in `/tmp` by default.
For persistent storage on Render, consider:
- Using Render Disk (paid plans)
- Switching to PostgreSQL (via Render PostgreSQL service)
- Using external storage (S3, etc.)

## 🔐 Security Notes

- Never commit API keys to the repository
- Use Render's environment variable secrets
- Enable HTTPS in Render dashboard
- Consider adding rate limiting for production

