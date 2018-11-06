package image

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/docker/cli/cli/command"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/integration-cli/daemon"
	"github.com/docker/docker/integration-cli/registry"
)

const registryURI = "127.0.0.1:5000"

// PATH=$PATH:`pwd`/balena-engine TESTDIRS="integration/image" TESTFLAGS="-test.run Delta" hack/make.sh test-integration
func TestDelta(t *testing.T) {
	type testCase struct {
		desc           string
		base           string
		target         string
		delta          string
		expectedImages []string
	}

	for _, c := range []testCase{
		{
			desc:           "busybox images",
			base:           "busybox:1.24",
			target:         "busybox:1.29",
			delta:          fmt.Sprintf("%s/delta-test:1.24-1.29", registryURI),
			expectedImages: []string{"busybox:1.24", fmt.Sprintf("%s/delta-test:1.24-1.29", registryURI)},
		},
	} {
		t.Run(c.desc, func(t *testing.T) {
			c := c

			var (
				err error
				rc  io.ReadCloser
			)

			d := daemon.New(t, "", "balena-engine-daemon", daemon.Config{})
			client, err := d.NewClient()
			if err != nil {
				t.Fatal(err)
			}

			var args = []string{}

			d.Start(t, args...)
			ctx := context.Background()

			reg, err := registry.NewV2(false, "htpasswd", "", registryURI)
			if err != nil {
				t.Fatal(err)
			}
			defer reg.Close()

			t.Log("Pulling delta base")
			rc, err = client.ImagePull(ctx,
				c.base,
				types.ImagePullOptions{})
			if err != nil {
				t.Fatal(err)
			}
			io.Copy(ioutil.Discard, rc)
			rc.Close()

			t.Log("Pulling delta target")
			rc, err = client.ImagePull(ctx,
				c.target,
				types.ImagePullOptions{})
			if err != nil {
				t.Fatal(err)
			}
			io.Copy(ioutil.Discard, rc)
			rc.Close()

			t.Log("Creating delta")
			var deltaID string
			rc, err = client.ImageDelta(ctx,
				c.base,
				c.target)
			if err != nil {
				t.Fatal(err)
			}
			{
				re := regexp.MustCompile("sha256:([a-fA-F0-9]{64})$")
				sc := bufio.NewScanner(rc)
				for sc.Scan() {
					var v map[string]interface{}
					if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
						t.Fatal(err)
					}
					if _, ok := v["errorDetail"]; ok {
						t.Fatal(errors.New(v["errorDetail"].(string)))
					}
					_, ok := v["progressDetail"]
					if ok {
						continue
					}
					s, ok := v["status"]
					if !ok {
						continue
					}
					status, ok := s.(string)
					if !ok {
						continue
					}
					r := re.FindStringSubmatch(status)
					if len(r) < 1 {
						continue
					}
					deltaID = r[0]
				}
				if err := sc.Err(); err != nil {
					t.Fatal(err)
				}
				if deltaID == "" {
					t.Fatal("Unable to parse delta image id from progress output")
				}
			}
			rc.Close()

			err = client.ImageTag(ctx, deltaID, c.delta)
			if err != nil {
				t.Fatal(err)
			}

			t.Log("Pushing delta to local registry")
			encodedAuth, err := command.EncodeAuthToBase64(types.AuthConfig{
				Username: reg.Username(),
				Password: reg.Password(),
				Auth:     "htpasswd",
			})
			if err != nil {
				t.Fatal(err)
			}
			rc, err = client.ImagePush(ctx, c.delta, types.ImagePushOptions{
				RegistryAuth: encodedAuth,
			})
			if err != nil {
				t.Fatal(err)
			}
			io.Copy(ioutil.Discard, rc)
			rc.Close()

			t.Log("Stopping daemon")
			d.Stop(t)

			args = append(args, []string{
				fmt.Sprintf("--delta-data-root=%s", d.Root),
				fmt.Sprintf("--delta-storage-driver=%s", os.Getenv("DOCKER_GRAPHDRIVER")),
			}...)
			var newRootDir = fmt.Sprintf("%s/root-before", d.Folder)
			d.Root = newRootDir

			t.Log("Starting daemon with separate delta-data-root")
			d.Start(t, args...)
			defer d.Stop(t)

			t.Log("Pulling delta from local registry")
			rc, err = client.ImagePull(ctx, c.delta, types.ImagePullOptions{
				RegistryAuth: encodedAuth,
			})
			if err != nil {
				t.Fatal(err)
			}
			{
				sc := bufio.NewScanner(rc)
				for sc.Scan() {
					var v map[string]interface{}
					if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
						t.Fatal(err)
					}
					if e, ok := v["errorDetail"]; ok {
						err, ok := e.(map[string]interface{})
						if !ok {
							t.Fail()
							break
						}
						t.Fatal(err["message"])
					}
				}
			}
			rc.Close()

			t.Log("Listing local images")
			// we need to check if the pull applied the delta cleanly
			var list []types.ImageSummary
			list, err = client.ImageList(ctx, types.ImageListOptions{})
			if err != nil {
				t.Fatal(err)
			}
			var foundImages []string
			for _, im := range list {
				t.Logf("%v, %v, %v", im.ID, im.Labels, im.RepoTags)
				foundImages = append(foundImages, im.RepoTags...)
			}

			assert.Equal(t, c.expectedImages, foundImages)
		})
	}
}
