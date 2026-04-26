# Claude Skills For Sales Director Monthly

These are custom Anthropic Skills for the Sales Director monthly workbook-to-deck workflow.

They follow Anthropic's current custom Skill structure:

- one directory per skill
- a `Skill.md` file with YAML frontmatter
- optional `resources/` files
- packaged as a ZIP with the skill folder at the ZIP root

## Skills

- `sd-workbook-fact-pack`
  - Excel-side factual extraction and monthly briefing
- `sd-powerpoint-builder`
  - PowerPoint-side executive deck authoring in the SimCorp template
- `sd-deck-audit`
  - PowerPoint-side deck QA against the validated fact pack

## Package

```bash
python3 scripts/package_claude_skills.py
```

ZIP outputs land in:

- `output/claude_skill_packages/`

## Upload

1. In Claude, go to `Customize > Skills`.
2. Click `+`.
3. Choose `Upload a skill`.
4. Upload the ZIP file for the skill you want.
5. Enable the skill in Claude, Excel, and PowerPoint.

These skills are designed to work with the monthly master builder:

- [run_sales_director_monthly_master_builder.py](/Users/test/crm-analytics/scripts/run_sales_director_monthly_master_builder.py)
