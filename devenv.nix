{
  pkgs,
  config,
  inputs,
  lib,
  ...
}:
{
  imports = [
    ./devenv/modules/nifi.nix
    (inputs.devenv-module-operaton + "/module.nix")
  ];

  options.services.operaton = {
    username = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      description = "Alias for services.operaton.adminUser.id";
    };
    password = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      description = "Alias for services.operaton.adminUser.password";
    };
  };

  config = {
    cachix.pull = [ "vasara-bpm" ];

    process.managers.process-compose.tui.enable = false;

    packages = [
      pkgs.python3
      pkgs.mailpit
    ];

    languages.java = {
      enable = true;
      jdk.package = pkgs.jdk21;
      maven.enable = true;
    };

    services.operaton = {
      enable = true;
      port = 8080;
      deployment = "${config.devenv.root}/deployment";
      basicAuth.enable = true;
      forwardHeadersStrategy = "native";
      package = inputs.devenv-module-operaton.packages.${pkgs.stdenv.hostPlatform.system}.default;
      adminUser = {
        id = if config.services.operaton.username != null then config.services.operaton.username else "demo";
        password = if config.services.operaton.password != null then config.services.operaton.password else "demo";
      };
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

    env = {
      NIFI_ADDR = "http://127.0.0.1:8081";
      VAULT_ADDR = lib.mkForce "http://127.0.0.1:8200";
      OPERATON_REST_URL = "http://localhost:8080/engine-rest";
      OPERATON_ADMIN_USER_ID = config.services.operaton.adminUser.id;
      OPERATON_ADMIN_USER_PASSWORD = config.services.operaton.adminUser.password;
    };

    processes.mailpit = {
      exec = "mailpit --listen 127.0.0.1:8025 --smtp 127.0.0.1:1025 --disable-version-check";
    };

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

    enterTest = ''
      set -eu
      cd "$DEVENV_ROOT"
      mvn -B test
    '';

    dotenv.disableHint = true;
  };
}
