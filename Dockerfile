FROM alpine:latest

ARG RCLONE_UID=1009
ARG RCLONE_GID=1009

RUN echo "**** Install Dependencies ****" && \
    apk add --no-cache \
        ca-certificates \
        fuse3 \
        mailcap \
        tzdata \
        unzip \
        coreutils && \
    echo "Enable user_allow_other in fuse" && \
    echo "user_allow_other" >> /etc/fuse.conf

ARG TARGETARCH
ARG TARGETVARIANT
ARG VERSION

COPY build/rclone-${VERSION}-linux-${TARGETARCH}${TARGETVARIANT:+-$TARGETVARIANT}.zip /tmp/rclone.zip

RUN unzip /tmp/rclone.zip -d /tmp && \
    mv /tmp/rclone-*-linux-${TARGETARCH}${TARGETVARIANT:+-$TARGETVARIANT}/rclone /usr/local/bin/rclone && \
    chmod +x /usr/local/bin/rclone && \
    rm -rf /tmp/rclone* && \
    apk del unzip

RUN addgroup -g "${RCLONE_GID}" rclone && adduser -u "${RCLONE_UID}" -Ds /bin/sh -G rclone rclone

ENTRYPOINT [ "rclone" ]

WORKDIR /data
ENV XDG_CONFIG_HOME=/config
