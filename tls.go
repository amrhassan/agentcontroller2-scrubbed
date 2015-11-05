package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io/ioutil"
	"net/http"
	"github.com/amrhassan/agentcontroller2/settings"
)

func configureClientCertificates(httpBinding settings.HTTPBinding, server *http.Server) (err error) {
	if httpBinding.ClientCertificateRequired() {
		server.TLSConfig.ClientAuth = tls.RequireAndVerifyClientCert
		server.TLSConfig.ClientCAs = x509.NewCertPool()
		for _, clientCA := range httpBinding.ClientCA {
			certPEM, err := ioutil.ReadFile(clientCA.Cert)
			if err != nil {
				return err
			}
			ok := server.TLSConfig.ClientCAs.AppendCertsFromPEM(certPEM)
			if !ok {
				return errors.New("failed to parse clientCA certificate")
			}
		}
	}
	return
}

func configureServerCertificates(httpBinding settings.HTTPBinding, server *http.Server) (err error) {
	server.TLSConfig.Certificates = make([]tls.Certificate, len(httpBinding.TLS), len(httpBinding.TLS))
	for certificateIndex, tlsSetting := range httpBinding.TLS {
		certificate, err := tls.LoadX509KeyPair(tlsSetting.Cert, tlsSetting.Key)
		if err != nil {
			return err
		}
		server.TLSConfig.Certificates[certificateIndex] = certificate
	}
	return
}
