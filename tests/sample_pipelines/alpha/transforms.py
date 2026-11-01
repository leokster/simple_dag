from simple_dag import JsonInput, JsonOutput, schedule, transform


@schedule(cron="0 6 * * *", timezone="Europe/Zurich")
@transform(out=JsonOutput("alpha/raw/numbers.json"))
def fetch_numbers(out: JsonOutput) -> None:
    out.write_data([1, 2, 3])


@schedule(on_upstream_success=True)
@transform(
    numbers=JsonInput("alpha/raw/numbers.json"),
    out=JsonOutput("alpha/curated/total.json", name="alpha_total"),
)
def total(numbers: list, out: JsonOutput) -> None:
    out.write_data({"total": sum(numbers)})
