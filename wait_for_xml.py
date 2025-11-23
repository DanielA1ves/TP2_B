from lxml import etree


def main():
    try:
        etree.parse("house_purchase.xml")
        return 0
    except Exception as exc:
        print(f"XML not ready: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
