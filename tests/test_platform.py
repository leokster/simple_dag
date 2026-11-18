from simple_dag import checks, datahandlers, definitions, execution, platform


def test_platform_still_offers_the_names_of_1_3_0():
    homes = {
        "ABCInput": datahandlers,
        "ABCOutput": datahandlers,
        "CheckResult": checks,
        "evaluate_check": checks,
        "ArrowInput": datahandlers,
        "ArrowOutput": datahandlers,
        "PROTOCOL_VERSION": definitions,
        "definition_manifest": definitions,
        "manifest_digest": definitions,
        "ExecutionContext": execution,
        "execute": execution,
    }
    assert set(platform.__all__) == set(homes)
    for name, home in homes.items():
        assert getattr(platform, name) is getattr(home, name)
