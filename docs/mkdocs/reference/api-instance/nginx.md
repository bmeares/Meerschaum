# 🚦 NGINX Configuration

Hosting Meerschaum behind NGINX is a great way set up TLS/SSL with Let's Encrypt (`certbot`). There are several important goals when setting up forwarding:

1. Proxy pass requests to your internal Meerschaum API instance (e.g. [http://localhost:8000](http://localhost:8000)).
2. Set upgrade headers for requests to locations ending in `/ws` or `/websocket`.
3. Increase the `client_max_body_size` limit (so larger chunks may be synced).

## Add a Subdomain

First, add an A record to your domain's DNS nameserver for the subdomain `mrsm` and set it to the public IPv4 address of your NGINX server.

!!! tip ""
    It's a good idea to add an AAAA DNS record as well for your server's IPv6 address.

Next, create `mrsm.conf` in `/etc/nginx/conf.d` (Ubuntu/Debian: `/etc/nginx/sites-available`) to add a new subdomain `mrsm`. Paste the example configuration below and replace `DOMAIN` with your root domain.

> This example assumes you are running NGINX on the same machine as your API instance (e.g. the [stack](/reference/stack)). Modify the proxy pass URLs based on your situation.

```nginx
server {
    server_name mrsm.DOMAIN;
    listen 80;
    listen [::]:80;

    client_max_body_size 100M;

    location / {
        proxy_pass http://localhost:8000;
    }

    location ~* /(ws|websocket)(/|$)
        proxy_pass http://localhost:8000$request_uri;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "Upgrade";
        proxy_read_timeout 86400;
        proxy_set_header Host $host;
        proxy_cache_bypass $http_upgrade;
    }
}
```

Reload NGINX to apply your new file:

```bash
sudo systemctl reload nginx
```

You should now be able to access `http://mrsm.your-domain.com`. If so, continue below to activate HTTPS for your subdomain:

## Add TLS/SSL

Thanks to [Let's Encrypt](https://letsencrypt.org/), adding HTTPS is easier than ever. Follow these steps:

1. Install [`certbot`](https://certbot.eff.org/).
2. Run `sudo certbot --nginx`. The wizard will ask which domain to add HTTPS to; select your new `mrsm.your-domain.com` or just press Enter.

If all went well, `https://mrsm.your-domain.com` should be active! Below is an example of what the modified `mrsm.conf` should look like:

```nginx
server {
    server_name mrsm.DOMAIN;
    client_max_body_size 100M;

    location / {
        proxy_pass http://localhost:8000;
    }
    
    location ~* /(ws|websocket)$ {
        proxy_pass http://localhost:8000$request_uri;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "Upgrade";
        proxy_read_timeout 86400;
        proxy_set_header Host $host;
        proxy_cache_bypass $http_upgrade;
    }

    listen [::]:443 ssl;
    listen 443 ssl; # managed by Certbot
    ssl_certificate /etc/letsencrypt/live/mrsm.DOMAIN/fullchain.pem; # managed by Certbot
    ssl_certificate_key /etc/letsencrypt/live/mrsm.DOMAIN/privkey.pem; # managed by Certbot
    include /etc/letsencrypt/options-ssl-nginx.conf; # managed by Certbot
    ssl_dhparam /etc/letsencrypt/ssl-dhparams.pem; # managed by Certbot
}

server {
    if ($host = mrsm.DOMAIN) {
        return 301 https://$host$request_uri;
    } # managed by Certbot


    server_name mrsm.DOMAIN;
    
    listen 80;
    listen [::]:80;
    return 404; # managed by Certbot
}
```