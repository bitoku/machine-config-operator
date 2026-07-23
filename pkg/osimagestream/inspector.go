package osimagestream

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/containers/common/pkg/retry"
	apicfgv1 "github.com/openshift/api/config/v1"
	mcfgv1 "github.com/openshift/api/machineconfiguration/v1"
	apioperatorsv1alpha1 "github.com/openshift/api/operator/v1alpha1"
	"github.com/openshift/machine-config-operator/pkg/imageutils"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

// isNetworkErrorRetryable checks if an error is a network-related error that should be retried.
// This handles DNS and timeout errors that may occur during cluster bootstrap.
func isNetworkErrorRetryable(err error) bool {
	if err == nil {
		return false
	}

	// Check for net.DNSError (DNS lookup failures)
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}

	// Check for timeout errors (net.Error with Timeout() == true)
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return false
}

// newImageInspectionRetryOptions creates retry options suitable for image inspection operations.
// It includes special handling for DNS and network errors that may occur during cluster bootstrap.
func newImageInspectionRetryOptions() *retry.Options {
	return &retry.Options{
		MaxRetry: 50,
		IsErrorRetryable: func(err error) bool {
			// Use the default retry logic first
			if retry.IsErrorRetryable(err) {
				return true
			}
			// Additionally check for network errors that may be wrapped in ways
			// that don't match the default retry logic's type assertions
			return isNetworkErrorRetryable(err)
		},
	}
}

// ImagesInspector provides methods for inspecting container images and extracting their contents.
type ImagesInspector interface {
	// Inspect retrieves metadata for one or more container images.
	Inspect(ctx context.Context, image ...string) ([]imageutils.BulkInspectResult, error)
	// FetchImageFile extracts and returns the contents of a file from a container image.
	FetchImageFile(ctx context.Context, image, path string) ([]byte, error)
}

// ImagesInspectorImpl is the default implementation of ImagesInspector.
// It lazily creates a SysContext on each operation via the provided factory.
type ImagesInspectorImpl struct {
	bulkInspector *imageutils.BulkInspector
	sysCtxFactory imageutils.SysContextFactory
}

// NewImagesInspector creates a new ImagesInspector with the given SysContext factory.
func NewImagesInspector(sysCtxFactory imageutils.SysContextFactory) *ImagesInspectorImpl {
	return &ImagesInspectorImpl{
		sysCtxFactory: sysCtxFactory,
		bulkInspector: imageutils.NewBulkInspector(&imageutils.BulkInspectorOptions{
			RetryOpts: newImageInspectionRetryOptions(),
			Count:     5,
			FailOnErr: false,
		}),
	}
}

// Inspect retrieves metadata for the specified container images.
func (i *ImagesInspectorImpl) Inspect(ctx context.Context, image ...string) ([]imageutils.BulkInspectResult, error) {
	sysCtx, err := i.sysCtxFactory()
	if err != nil {
		return nil, fmt.Errorf("creating system context for image inspection: %w", err)
	}
	defer func() {
		if cleanupErr := sysCtx.Cleanup(); cleanupErr != nil {
			klog.Warningf("could not clean up system context: %v", cleanupErr)
		}
	}()
	return i.bulkInspector.Inspect(ctx, sysCtx.SysContext, image...)
}

// FetchImageFile extracts and returns the contents of a file from the container image.
func (i *ImagesInspectorImpl) FetchImageFile(ctx context.Context, image, path string) ([]byte, error) {
	sysCtx, err := i.sysCtxFactory()
	if err != nil {
		return nil, fmt.Errorf("creating system context for image file fetch: %w", err)
	}
	defer func() {
		if cleanupErr := sysCtx.Cleanup(); cleanupErr != nil {
			klog.Warningf("could not clean up system context: %v", cleanupErr)
		}
	}()
	targetHeaderPath := strings.TrimLeft(path, "./")
	return imageutils.ReadImageFileContent(ctx, sysCtx.SysContext, image, func(header *tar.Header) bool {
		return targetHeaderPath == strings.TrimLeft(header.Name, "./")
	}, newImageInspectionRetryOptions())
}

// InspectStreamClassWith inspects a container image and returns its OS stream
// class (e.g. "rhel-9", "rhel-10") from the image's labels. Returns ("", nil)
// when the image has no stream class label.
// TODO(OCP 5.3): Remove when runc is removed.
func InspectStreamClassWith(ctx context.Context, inspector ImagesInspector, imageURL string) (string, error) {
	results, err := inspector.Inspect(ctx, imageURL)
	if err != nil {
		return "", fmt.Errorf("failed to inspect OS image: %w", err)
	}
	if len(results) == 0 {
		return "", fmt.Errorf("no inspection result for OS image")
	}
	if results[0].Error != nil {
		return "", fmt.Errorf("failed to inspect OS image: %w", results[0].Error)
	}
	if results[0].InspectInfo == nil {
		return "", nil
	}

	extractor := NewImageStreamExtractor()
	imageData := extractor.GetImageData(imageURL, results[0].InspectInfo.Labels)
	if imageData == nil {
		return "", nil
	}
	return imageData.Stream, nil
}

// InspectStreamClassWithMirrors builds an image system context with registry mirror
// rules applied, then inspects the container image to determine its OS stream class.
// TODO(OCP 5.3): Remove when runc is removed.
func InspectStreamClassWithMirrors(
	ctx context.Context,
	secret *corev1.Secret,
	cc *mcfgv1.ControllerConfig,
	imgCfg *apicfgv1.Image,
	icspRules []*apioperatorsv1alpha1.ImageContentSourcePolicy,
	idmsRules []*apicfgv1.ImageDigestMirrorSet,
	itmsRules []*apicfgv1.ImageTagMirrorSet,
	imageURL string,
) (string, error) {
	sysCtxFactory := func() (*imageutils.SysContext, error) {
		builder := imageutils.NewSysContextBuilder().
			WithSecret(secret).
			WithControllerConfig(cc)

		registriesConfig, err := imageutils.GenerateRegistriesConfig(imgCfg, icspRules, idmsRules, itmsRules)
		if err != nil {
			return nil, fmt.Errorf("failed to generate registries config: %w", err)
		}
		if registriesConfig != nil {
			builder.WithRegistriesConfig(registriesConfig)
		}

		return builder.Build()
	}

	return InspectStreamClassWith(ctx, NewImagesInspector(sysCtxFactory), imageURL)
}

// ImagesInspectorFactory creates ImagesInspector instances.
type ImagesInspectorFactory interface {
	ForContext(sysCtxFactory imageutils.SysContextFactory) ImagesInspector
}

// DefaultImagesInspectorFactory is the production implementation of ImagesInspectorFactory.
type DefaultImagesInspectorFactory struct{}

// ForContext creates an ImagesInspector with the given SysContext factory.
func (f *DefaultImagesInspectorFactory) ForContext(sysCtxFactory imageutils.SysContextFactory) ImagesInspector {
	return NewImagesInspector(sysCtxFactory)
}
