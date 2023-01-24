def make_exe():
    dist = default_python_distribution()

    policy = dist.make_python_packaging_policy()
    policy.resources_location_fallback = "filesystem-relative:lib"

    python_config = dist.make_python_interpreter_config()
    python_config.run_module = "pgactivity"

    exe = dist.to_python_executable(
        name="pg_activity",
        packaging_policy=policy,
        config=python_config,
    )

    for resource in exe.pip_install(["psutil==5.9.4"]):
        resource.add_location = "filesystem-relative:lib"
        exe.add_python_resource(resource)

    exe.add_python_resources(
        exe.pip_install(
            [".", "-r", ".requirements-pyoxidizer.txt"],
        )
    )

    return exe

register_target("exe", make_exe)

resolve_targets()
