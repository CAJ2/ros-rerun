ARG ROS_DISTRO=jazzy
FROM ros:$ROS_DISTRO AS base
ARG DEBIAN_FRONTEND=noninteractive
ARG PIXI_VERSION=v0.49.0

# Install dependencies
RUN apt-get update && apt-get install -y \
    curl \
    git \
    libclang-dev \
    tmux \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain 1.85.0 -y
ENV PATH=/root/.cargo/bin:$PATH
RUN . /root/.cargo/env && cargo install --git https://github.com/ros2-rust/cargo-ament-build

# Install Pixi
RUN curl -L -o /usr/local/bin/pixi -fsSL --compressed "https://github.com/prefix-dev/pixi/releases/download/${PIXI_VERSION}/pixi-$(uname -m)-unknown-linux-musl" \
    && chmod +x /usr/local/bin/pixi \
    && pixi info

# Install the colcon-cargo and colcon-ros-cargo plugins
RUN pip install --break-system-packages pytest colcon-ros-cargo vcs2l

RUN echo 'source /root/.cargo/env' >> /root/.bashrc
RUN echo 'eval "$(pixi completion -s bash)"' >> /root/.bashrc
RUN echo 'source /workspaces/rerun-ros/install/setup.bash' >> /root/.bashrc
