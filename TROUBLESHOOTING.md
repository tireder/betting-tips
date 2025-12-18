# 🔧 Troubleshooting Guide

## JavaScript Module Loading Error

### Error Message
```
TypeError: Failed to fetch dynamically imported module: 
https://betting-tips-nr12.onrender.com/static/js/index.XXXXX.js
```

### Causes
1. **CORS disabled** - Streamlit needs CORS enabled for Render.com
2. **Static files not served** - Build artifacts missing
3. **Port mismatch** - PORT env var not matching Streamlit config
4. **Cache issues** - Browser or Render cache

### Solutions

#### ✅ Solution 1: Update Configuration (RECOMMENDED)

The updated `Dockerfile` and `.streamlit/config.toml` now have:
- `enableCORS = true` (was `false`)
- Proper environment variables set
- Correct port handling

**Steps:**
1. Pull latest changes
2. Rebuild on Render.com
3. Clear browser cache (Ctrl+Shift+Delete)
4. Try again

#### ✅ Solution 2: Use Dockerfile.fixed

If the main Dockerfile still has issues:

1. Rename `Dockerfile.fixed` to `Dockerfile`
2. Rebuild on Render
3. This version uses a more robust startup script

#### ✅ Solution 3: Manual Environment Variables

In Render.com dashboard, add these environment variables:

```
STREAMLIT_SERVER_ENABLE_CORS=true
STREAMLIT_SERVER_ENABLE_XSRF_PROTECTION=true
STREAMLIT_SERVER_HEADLESS=true
STREAMLIT_BROWSER_GATHER_USAGE_STATS=false
```

#### ✅ Solution 4: Check Render Logs

1. Go to Render dashboard
2. Click on your service
3. Check "Logs" tab
4. Look for:
   - Port binding errors
   - CORS errors
   - Static file serving errors

#### ✅ Solution 5: Verify Streamlit Version

Ensure you're using a recent Streamlit version (≥1.30.0):

```bash
# In requirements.txt
streamlit>=1.30.0
```

Then rebuild.

### 🔍 Debugging Steps

1. **Check if app starts:**
   ```bash
   # In Render logs, you should see:
   "Starting Streamlit on port 8501..."
   ```

2. **Verify static files:**
   - Open browser DevTools (F12)
   - Check Network tab
   - Look for 404 errors on `.js` files

3. **Test health endpoint:**
   ```
   https://your-app.onrender.com/_stcore/health
   ```
   Should return: `{"status": "ok"}`

4. **Check CORS headers:**
   - In DevTools → Network
   - Check Response Headers
   - Should see: `Access-Control-Allow-Origin: *`

### 🚨 Common Issues

#### Issue: "Port already in use"
**Fix:** Ensure PORT env var matches Streamlit config

#### Issue: "Static files 404"
**Fix:** 
- Clear Render build cache
- Rebuild from scratch
- Check `.dockerignore` isn't excluding needed files

#### Issue: "CORS error in console"
**Fix:** 
- Set `enableCORS = true` in config
- Add CORS env vars
- Rebuild

### 📞 Still Not Working?

1. **Check Render Status:**
   - Render.com status page
   - Your service status (should be "Live")

2. **Try Different Browser:**
   - Chrome/Edge
   - Firefox
   - Incognito mode

3. **Check Render Plan:**
   - Free tier has limitations
   - May need Starter plan for better reliability

4. **Contact Support:**
   - Render.com support
   - Include logs and error messages

---

## Other Common Errors

### "Module not found" errors
- Check `requirements.txt` has all dependencies
- Rebuild Docker image

### "API key not found"
- Set environment variables in Render dashboard
- Don't commit keys to git

### "Database locked"
- SQLite cache issue
- Consider using Render PostgreSQL for production

### App sleeps after 15 minutes
- Free tier limitation
- Upgrade to Starter plan for always-on

