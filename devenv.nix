{
  pkgs,
  config,
  inputs,
  ...
}:
{
  imports = [
    ./devenv/modules/nifi.nix
    (inputs.devenv-module-operaton + "/module.nix")
  ];

  process.managers.process-compose.tui.enable = false;

  packages = [
    pkgs.python3
  ];

  languages.java = {
    enable = true;
    jdk.package = pkgs.jdk21;
    maven.enable = true;
  };

  services.operaton = {
    enable = true;
    port = 8080;
    forwardHeadersStrategy = "native";
    package = inputs.devenv-module-operaton.packages.${pkgs.stdenv.hostPlatform.system}.default;
  };

  services.vault = {
    enable = true;
    address = "127.0.0.1:8200";
    ui = true;
  };

  services.nifi = {
    enable = true;
    port = 8081;
    narPaths = [
      "nifi-controller/nifi-controller-api-nar/target"
      "nifi-controller/nifi-controller-nar/target"
      "nifi-processor/nifi-processor-nar/target"
    ];
  };

  env.NIFI_ADDR = "http://127.0.0.1:8081";

  processes.preconfigure = {
    exec = "python3 $DEVENV_ROOT/devenv/scripts/preconfigure.py";
  };

  scripts.preconfigure.exec = ''
    python3 "$DEVENV_ROOT/devenv/scripts/preconfigure.py"
  '';

  scripts.build.exec = ''
    set -eu
    cd "$DEVENV_ROOT"
    mvn -B package "$@"
  '';

  dotenv.disableHint = true;
}
