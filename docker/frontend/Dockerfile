FROM nginx:1.27-alpine

WORKDIR /usr/share/nginx/html

COPY nginx.conf /etc/nginx/nginx.conf

# Копируем новые страницы
COPY day.html  ./day.html
COPY week.html ./week.html
COPY index.html ./index.html
COPY menu.html  ./menu.html
COPY teacher.html ./teacher.html
COPY admin.html   ./admin.html


EXPOSE 80
