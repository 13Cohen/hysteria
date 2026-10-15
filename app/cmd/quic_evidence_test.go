package cmd

import (
	"strings"
	"testing"
	"time"

	"github.com/apernet/hysteria/core/v2/server"
	"github.com/spf13/viper"
)

func TestServerQUICEvidenceConfig(t *testing.T) {
	v := viper.New()
	v.SetConfigType("yaml")
	if err := v.ReadConfig(strings.NewReader("quicEvidence:\n  enabled: true\n  interval: 7s\n  maxConnections: 42\n  streamEvidence: true\n  udpEvidence: true\n  packetEvidence: false\n  maxStreams: 100\n  maxStreamsPerConnection: 12\n")); err != nil {
		t.Fatal(err)
	}
	var config serverConfig
	if err := v.Unmarshal(&config); err != nil {
		t.Fatal(err)
	}
	var core server.Config
	if err := config.fillQUICEvidence(&core); err != nil {
		t.Fatal(err)
	}
	if !core.QUICEvidenceConfig.Enabled || core.QUICEvidenceConfig.Interval != 7*time.Second || core.QUICEvidenceConfig.MaxConnections != 42 || core.QUICEvidenceLogger == nil || !core.QUICEvidenceConfig.StreamEvidence || !core.QUICEvidenceConfig.UDPEvidence || core.QUICEvidenceConfig.PacketEvidence || core.QUICEvidenceConfig.MaxStreams != 100 || core.QUICEvidenceConfig.MaxStreamsPerConnection != 12 {
		t.Fatalf("bad parsed config: %+v", core.QUICEvidenceConfig)
	}
	config.QUICEvidence.Enabled = false
	core = server.Config{}
	if err := config.fillQUICEvidence(&core); err != nil || core.QUICEvidenceLogger != nil {
		t.Fatal("disabled option installed an emitter")
	}
}
