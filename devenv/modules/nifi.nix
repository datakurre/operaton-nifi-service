{
  pkgs,
  config,
  lib,
  ...
}:

let
  cfg = config.services.nifi;
  inherit (lib) types mkOption mkEnableOption;

  # NiFi home is assembled at runtime under DEVENV_STATE, because NiFi writes
  # its repositories, logs and flow definition relative to it.
  nifiHome = "${config.env.DEVENV_STATE}/nifi";

  # nixpkgs only packages NiFi 1.x; extensions in this repository target the
  # NiFi 2.x API, so the official binary distribution is packaged here.
  nifi = pkgs.stdenvNoCC.mkDerivation rec {
    pname = "nifi";
    version = "2.4.0";
    src = pkgs.fetchurl {
      url = "https://archive.apache.org/dist/nifi/${version}/nifi-${version}-bin.zip";
      sha512 = "a3de6ced74371bd8b1e95267c37dd526ef6d653c038e6a9e4a5f779ee767eb7eec1a5fd3cfcb0b764ce2b7c76e4f9f0dd848fa024796876340afb423fd1a81db";
    };
    nativeBuildInputs = [ pkgs.unzip ];
    installPhase = ''
      mkdir -p $out/share
      cp -r . $out/share/nifi
    '';
  };
in
{
  options.services.nifi = {
    enable = mkEnableOption "Apache NiFi service";

    package = mkOption {
      type = types.package;
      default = nifi;
      description = "NiFi binary distribution (expects share/nifi inside).";
    };

    jdk = mkOption {
      type = types.package;
      default = pkgs.jdk21;
      description = "JDK used to run NiFi (NiFi 2.x requires Java 21).";
    };

    host = mkOption {
      type = types.str;
      default = "127.0.0.1";
    };

    port = mkOption {
      type = types.port;
      default = 8080;
    };

    heap = mkOption {
      type = types.str;
      default = "1g";
      description = "Initial and maximum JVM heap size.";
    };

    extensionsDir = mkOption {
      type = types.str;
      default = "${nifiHome}/extensions";
      description = ''
        NAR autoload directory (see the nifi-deploy script). New NARs copied
        here are loaded by a running NiFi; rebuilt versions of already loaded
        NARs are loaded when NiFi is restarted.
      '';
    };

    narPaths = mkOption {
      type = types.listOf types.str;
      default = [ ];
      example = [ "nifi-controller/nifi-controller-nar/target" ];
      description = ''
        Directories (relative to DEVENV_ROOT, or absolute) searched for *.nar
        files by the nifi-deploy script.
      '';
    };

    proxyHost = mkOption {
      type = types.str;
      default = "localhost:${toString cfg.port},127.0.0.1:${toString cfg.port},*.app.github.dev,*.github.dev";
      description = "Allowed Host header values (comma-separated), for reverse proxies and GitHub Codespaces.";
    };
  };

  config = lib.mkIf cfg.enable {
    packages = [
      cfg.jdk
      pkgs.maven
    ];

    env = {
      NIFI_HOME = nifiHome;
      NIFI_LOG_DIR = "${nifiHome}/logs";
      NIFI_PID_DIR = "${nifiHome}/run";
      NIFI_EXTENSIONS_DIR = cfg.extensionsDir;
    };

    scripts.nifi-deploy = {
      description = "Copy built NARs into the NiFi autoload directory";
      exec = ''
        set -eu
        mkdir -p "${cfg.extensionsDir}"
        cd "$DEVENV_ROOT"
        found=0
        for dir in ${lib.escapeShellArgs cfg.narPaths}; do
          for nar in "$dir"/*.nar; do
            [ -e "$nar" ] || continue
            cp -v "$nar" "${cfg.extensionsDir}/"
            found=1
          done
        done
        [ "$found" = 1 ] || echo "No NARs found; run 'mvn package' first." >&2
      '';
    };

    processes.nifi = {
      exec = ''
        set -eu
        dist=${cfg.package}/share/nifi
        home=${lib.escapeShellArg nifiHome}
        mkdir -p "$home"/{run,logs,conf} "${cfg.extensionsDir}"

        # lib/ mirrors the immutable distribution as a tree of symlinks (NiFi
        # does not follow a symlinked lib/ itself); conf/ is refreshed from it on
        # every start so that edits below stay idempotent.
        if [ -d "$home"/lib ] && [ ! -L "$home"/lib ]; then chmod -R u+w "$home"/lib; fi
        rm -rf "$home"/bin "$home"/lib
        cp -rs $dist/lib "$home"/lib

        # Build NARs first if none exist yet (e.g. fresh clone)
        has_nars=0
        for dir in ${lib.escapeShellArgs cfg.narPaths}; do
          case "$dir" in
            /*) nar_pattern="$dir"/*.nar ;;
            *) nar_pattern="$DEVENV_ROOT/$dir"/*.nar ;;
          esac
          for nar in $nar_pattern; do
            if [ -e "$nar" ]; then
              has_nars=1
              break
            fi
          done
        done
        if [ "$has_nars" = 0 ]; then
          echo "No built NARs found; running 'mvn -B package -DskipTests'..."
          (cd "$DEVENV_ROOT" && JAVA_HOME=${cfg.jdk} mvn -B package -DskipTests)
        fi

        # Deploy built NARs into extensions directory
        for dir in ${lib.escapeShellArgs cfg.narPaths}; do
          case "$dir" in
            /*) nar_pattern="$dir"/*.nar ;;
            *) nar_pattern="$DEVENV_ROOT/$dir"/*.nar ;;
          esac
          for nar in $nar_pattern; do
            [ -e "$nar" ] || continue
            cp -u "$nar" "${cfg.extensionsDir}/"
          done
        done

        # NiFi loads previously unpacked NARs before scanning the autoload
        # directory and then skips NARs with the same coordinates, so rebuilt
        # (e.g. SNAPSHOT) NARs would never be picked up without this.
        for nar in "${cfg.extensionsDir}"/*.nar; do
          [ -e "$nar" ] || continue
          rm -rf "$home/work/nar/extensions/$(basename "$nar")-unpacked"
        done
        for f in $dist/conf/*; do
          name=$(basename "$f")
          case "$name" in
            flow.json.gz|flow.xml.gz) continue ;;
          esac
          cp -rf "$f" "$home"/conf/
        done
        chmod -R u+w "$home"/conf

        proxy_host="${cfg.proxyHost}"
        if [ -n "''${CODESPACE_NAME:-}" ]; then
          cs_domain="''${GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN:-app.github.dev}"
          cs_host="''${CODESPACE_NAME}-${toString cfg.port}.''${cs_domain}"
          proxy_host="''${proxy_host},''${cs_host},''${cs_host}:443"
        fi

        # Plain HTTP without authentication: suitable for local development only.
        sed -i "$home"/conf/nifi.properties \
          -e 's|^nifi.web.http.host=.*|nifi.web.http.host=${cfg.host}|' \
          -e 's|^nifi.web.http.port=.*|nifi.web.http.port=${toString cfg.port}|' \
          -e 's|^nifi.web.https.host=.*|nifi.web.https.host=|' \
          -e 's|^nifi.web.https.port=.*|nifi.web.https.port=|' \
          -e "s|^nifi.web.proxy.host=.*|nifi.web.proxy.host=$proxy_host|" \
          -e 's|^nifi.remote.input.secure=.*|nifi.remote.input.secure=false|' \
          -e 's|^nifi.security.keystore=.*|nifi.security.keystore=|' \
          -e 's|^nifi.security.keystoreType=.*|nifi.security.keystoreType=|' \
          -e 's|^nifi.security.keystorePasswd=.*|nifi.security.keystorePasswd=|' \
          -e 's|^nifi.security.keyPasswd=.*|nifi.security.keyPasswd=|' \
          -e 's|^nifi.security.truststore=.*|nifi.security.truststore=|' \
          -e 's|^nifi.security.truststoreType=.*|nifi.security.truststoreType=|' \
          -e 's|^nifi.security.truststorePasswd=.*|nifi.security.truststorePasswd=|' \
          -e 's|^nifi.security.user.authorizer=.*|nifi.security.user.authorizer=|' \
          -e 's|^nifi.security.user.login.identity.provider=.*|nifi.security.user.login.identity.provider=|' \
          -e 's|^nifi.sensitive.props.key=.*|nifi.sensitive.props.key=devenv-nifi-sensitive-props-key|' \
          -e 's|^nifi.nar.library.autoload.directory=.*|nifi.nar.library.autoload.directory=${cfg.extensionsDir}|'

        sed -i "$home"/conf/bootstrap.conf \
          -e 's|^java.arg.2=.*|java.arg.2=-Xms${cfg.heap}|' \
          -e 's|^java.arg.3=.*|java.arg.3=-Xmx${cfg.heap}|'

        # nifi-env.sh ignores NIFI_HOME etc. unless NIFI_OVERRIDE_NIFIENV is set.
        export JAVA_HOME=${cfg.jdk}
        export NIFI_OVERRIDE_NIFIENV=true
        export NIFI_HOME="$home"
        export NIFI_LOG_DIR="$home"/logs
        export NIFI_PID_DIR="$home"/run
        cd "$home"
        echo "Starting NiFi on http://${cfg.host}:${toString cfg.port}/nifi/"
        exec bash $dist/bin/nifi.sh run
      '';
      ready = {
        http.get = {
          host = cfg.host;
          port = cfg.port;
          path = "/nifi-api/system-diagnostics";
        };
        period = 5;
        failure_threshold = 20;
      };
    };

    enterShell = ''
      echo "NiFi ${cfg.package.version or ""}: http://${cfg.host}:${toString cfg.port}/nifi/ (start with: devenv up nifi)"
    '';
  };
}
