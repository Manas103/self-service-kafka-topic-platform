// Package docker is a minimal client for the real Docker Engine API, over
// the real /var/run/docker.sock, with no third-party Go module: this control
// plane's "AWS SDK (ECS)" equivalent for the local provisioning path is a
// hand-written REST client against the same kind of container-orchestration
// API surface (create a task/container, start it, inspect its health), the
// same relationship moto has to boto3 elsewhere in this portfolio, except
// here the backend (the Docker daemon) is real, not mocked; see README
// Honest framing for why ECS itself is not exercised in this build.
package docker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

const apiVersion = "v1.44"

type Client struct {
	http *http.Client
}

// NewClient dials the real Docker daemon's Unix socket. No TCP, no mock.
func NewClient(socketPath string) *Client {
	return &Client{
		http: &http.Client{
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return net.Dial("unix", socketPath)
				},
			},
			Timeout: 30 * time.Second,
		},
	}
}

type ContainerSpec struct {
	Name     string
	Image    string
	Port     int // the port the image listens on inside the container
	HostPort int // the host loopback port it is published to; defaults to Port if zero
	Env      map[string]string
	CPUUnits int // 1024 == 1 vCPU, the same unit ECS uses for task CPU
	MemoryMB int
}

type createContainerRequest struct {
	Image        string              `json:"Image"`
	Env          []string            `json:"Env"`
	ExposedPorts map[string]struct{} `json:"ExposedPorts"`
	HostConfig   hostConfig          `json:"HostConfig"`
}

type hostConfig struct {
	PortBindings map[string][]portBinding `json:"PortBindings"`
	Memory       int64                     `json:"Memory"`  // bytes
	CPUShares    int                       `json:"CpuShares"`
}

type portBinding struct {
	HostIP   string `json:"HostIp"`
	HostPort string `json:"HostPort"`
}

type createContainerResponse struct {
	ID       string   `json:"Id"`
	Warnings []string `json:"Warnings"`
}

// CreateAndStart creates a container from spec, publishes its container port
// to the same port number on 127.0.0.1, and starts it. Returns the real
// Docker container ID.
func (c *Client) CreateAndStart(ctx context.Context, spec ContainerSpec) (string, error) {
	if err := c.pullIfMissing(ctx, spec.Image); err != nil {
		return "", err
	}

	hostPort := spec.HostPort
	if hostPort == 0 {
		hostPort = spec.Port
	}
	portKey := fmt.Sprintf("%d/tcp", spec.Port)
	env := make([]string, 0, len(spec.Env))
	for k, v := range spec.Env {
		env = append(env, k+"="+v)
	}

	reqBody := createContainerRequest{
		Image: spec.Image,
		Env:   env,
		ExposedPorts: map[string]struct{}{
			portKey: {},
		},
		HostConfig: hostConfig{
			PortBindings: map[string][]portBinding{
				portKey: {{HostIP: "127.0.0.1", HostPort: fmt.Sprintf("%d", hostPort)}},
			},
			Memory:    int64(spec.MemoryMB) * 1024 * 1024,
			CPUShares: spec.CPUUnits,
		},
	}

	var created createContainerResponse
	if err := c.postJSON(ctx, fmt.Sprintf("/containers/create?name=%s", spec.Name), reqBody, &created); err != nil {
		return "", fmt.Errorf("create container: %w", err)
	}

	if err := c.post(ctx, fmt.Sprintf("/containers/%s/start", created.ID), nil); err != nil {
		return "", fmt.Errorf("start container: %w", err)
	}
	return created.ID, nil
}

func (c *Client) pullIfMissing(ctx context.Context, image string) error {
	// Docker's create endpoint fails if the image is not present locally;
	// /images/create pulls it (a no-op, fast, if it already is).
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://docker/"+apiVersion+"/images/create?fromImage="+image, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(splitImageRef(req, image))
	if err != nil {
		return fmt.Errorf("pulling image: %w", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("pulling image %s: HTTP %d: %s", image, resp.StatusCode, string(data))
	}
	return nil
}

// splitImageRef rewrites the pull request's query to separate image name
// from tag, matching the Docker API's fromImage/tag parameters.
func splitImageRef(req *http.Request, image string) *http.Request {
	name, tag := image, "latest"
	for i := len(image) - 1; i >= 0; i-- {
		if image[i] == ':' {
			name, tag = image[:i], image[i+1:]
			break
		}
	}
	q := req.URL.Query()
	q.Set("fromImage", name)
	q.Set("tag", tag)
	req.URL.RawQuery = q.Encode()
	return req
}

// Stop and Remove tear down exactly the container this process started, by
// the ID it was given back when it created it, never by name/label scanning.
func (c *Client) Stop(ctx context.Context, id string) error {
	return c.post(ctx, "/containers/"+id+"/stop?t=5", nil)
}

func (c *Client) Remove(ctx context.Context, id string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		"http://docker/"+apiVersion+"/containers/"+id+"?force=true", nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(data))
	}
	return nil
}

func (c *Client) post(ctx context.Context, path string, body io.Reader) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://docker/"+apiVersion+path, body)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(data))
	}
	return nil
}

func (c *Client) postJSON(ctx context.Context, path string, in, out interface{}) error {
	b, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://docker/"+apiVersion+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(data))
	}
	if out != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}
