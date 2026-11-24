/**
 * What changed and what comes next, as the front page tells it (the author,
 * 2026-11-24: useful to whoever comes back). One entry per change a visitor
 * would notice, newest first, written in all five locales of the site. Add an
 * entry with the deploy that ships the change.
 */

export const LOCALES = ["es", "en", "de", "pt", "zh"] as const;
export type Locale = (typeof LOCALES)[number];
export type Localized = Record<Locale, string>;

export type Change = { date: string; text: Localized };
export type Plan = { text: Localized };

export const CHANGES: Change[] = [
  {
    date: "2026-11-24",
    text: {
      es: "Portada nueva: primero la herramienta, después las cifras en vivo, lo que ha cambiado y lo que viene.",
      en: "A new front page: the tool first, then live figures, what has changed and what comes next.",
      de: "Neue Startseite: zuerst das Werkzeug, dann aktuelle Zahlen, was sich geändert hat und was als Nächstes kommt.",
      pt: "Página inicial nova: primeiro a ferramenta, depois os números ao vivo, o que mudou e o que vem a seguir.",
      zh: "全新首页：先是工具，然后是实时数据、最新变化和后续计划。",
    },
  },
  {
    date: "2026-11-24",
    text: {
      es: "Anotar una secuencia vuelve a funcionar: cada predicción lee el grafo GO de las anotaciones que transfiere.",
      en: "Annotating a sequence works again: each prediction reads the GO graph of the annotations it transfers.",
      de: "Das Annotieren einer Sequenz funktioniert wieder: Jede Vorhersage liest den GO-Graphen der Annotationen, die sie überträgt.",
      pt: "Anotar uma sequência volta a funcionar: cada previsão lê o grafo GO das anotações que transfere.",
      zh: "序列注释功能已恢复：每个预测都读取其所转移注释对应的 GO 图。",
    },
  },
  {
    date: "2026-11-24",
    text: {
      es: "La web funciona sobre una base limpia, reconstruida desde cero con operaciones declaradas y comprobada contra desarrollo: seis versiones de las anotaciones GO, de 2016 a 2025, los metadatos de UniProt y ocho modelos de lenguaje de proteínas.",
      en: "The site runs on a clean store, rebuilt from scratch by declared operations and checked against development: six releases of the GO annotations, from 2016 to 2025, UniProt metadata and eight protein language models.",
      de: "Die Seite läuft auf einem sauberen Datenbestand, von Grund auf mit deklarierten Operationen neu aufgebaut und gegen die Entwicklungsumgebung geprüft: sechs Versionen der GO-Annotationen von 2016 bis 2025, UniProt-Metadaten und acht Protein-Sprachmodelle.",
      pt: "O site funciona sobre uma base limpa, reconstruída do zero com operações declaradas e verificada contra o desenvolvimento: seis versões das anotações GO, de 2016 a 2025, os metadados do UniProt e oito modelos de linguagem de proteínas.",
      zh: "网站现运行在一个干净的数据库上：通过声明的操作从零重建，并与开发环境逐项核对，包括 2016 至 2025 年的六个 GO 注释版本、UniProt 元数据和八个蛋白质语言模型。",
    },
  },
];

export const PLANS: Plan[] = [
  {
    text: {
      es: "Etapa 1, lo que cada proteína ya sabe: sus anotaciones existentes como línea base pura, y esas mismas anotaciones ordenadas por un modelo aprendido.",
      en: "Stage 1, what each protein already knows: its existing annotations as a pure baseline, and the same annotations ranked by a learned model.",
      de: "Stufe 1, was jedes Protein bereits weiß: seine vorhandenen Annotationen als reine Basislinie und dieselben Annotationen, geordnet von einem gelernten Modell.",
      pt: "Etapa 1, o que cada proteína já sabe: as suas anotações existentes como linha de base pura e essas mesmas anotações ordenadas por um modelo aprendido.",
      zh: "第一阶段：每个蛋白质已有的知识。以其现有注释作为纯基线，并用学习模型对这些注释排序。",
    },
  },
  {
    text: {
      es: "Etapa 2, los ocho modelos de lenguaje de proteínas evaluados en crudo, desglosados por lo que se sabe de cada proteína, por identidad de secuencia y por longitud.",
      en: "Stage 2, the eight protein language models evaluated raw, broken down by how much is known about each protein, by sequence identity and by length.",
      de: "Stufe 2, die acht Protein-Sprachmodelle in der Rohauswertung, aufgeschlüsselt nach dem Vorwissen über jedes Protein, nach Sequenzidentität und nach Länge.",
      pt: "Etapa 2, os oito modelos de linguagem de proteínas avaliados em bruto, desagregados pelo que se sabe de cada proteína, pela identidade de sequência e pelo comprimento.",
      zh: "第二阶段：对八个蛋白质语言模型进行原始评估，并按每个蛋白质的已知程度、序列一致性和长度分层报告。",
    },
  },
  {
    text: {
      es: "Después, una publicación cada noche con el mejor método para cada tipo de proteína.",
      en: "Later, a nightly publication of the best method for each kind of protein.",
      de: "Danach eine nächtliche Veröffentlichung mit der besten Methode für jede Art von Protein.",
      pt: "Depois, uma publicação todas as noites com o melhor método para cada tipo de proteína.",
      zh: "之后：每晚发布针对每类蛋白质的最佳方法的结果。",
    },
  },
];

/** The text in the reader's locale, or the English one for a locale the site does not carry. */
export function inLocale(text: Localized, locale: string): string {
  return (LOCALES as readonly string[]).includes(locale) ? text[locale as Locale] : text.en;
}
