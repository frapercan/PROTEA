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
      es: "El instrumento sirve un universo nuevo, reconstruido desde cero: 2.758.480 proteínas, 113.487.299 anotaciones GO en seis versiones de 2016 a 2025, los metadatos de UniProt y 20.191.896 vectores de ocho modelos de lenguaje de proteínas.",
      en: "The instrument now serves a new universe, rebuilt from scratch: 2,758,480 proteins, 113,487,299 GO annotations in six releases from 2016 to 2025, UniProt metadata, and 20,191,896 vectors from eight protein language models.",
      de: "Das Instrument bedient ein neues, von Grund auf neu aufgebautes Universum: 2.758.480 Proteine, 113.487.299 GO-Annotationen in sechs Versionen von 2016 bis 2025, UniProt-Metadaten und 20.191.896 Vektoren aus acht Protein-Sprachmodellen.",
      pt: "O instrumento serve um universo novo, reconstruído do zero: 2.758.480 proteínas, 113.487.299 anotações GO em seis versões de 2016 a 2025, os metadados do UniProt e 20.191.896 vetores de oito modelos de linguagem de proteínas.",
      zh: "本工具现使用一个从零重建的新数据集：2,758,480 个蛋白质、2016 至 2025 年六个版本中的 113,487,299 条 GO 注释、UniProt 元数据，以及来自八个蛋白质语言模型的 20,191,896 个向量。",
    },
  },
  {
    date: "2026-11-24",
    text: {
      es: "Cada pieza se comparó con desarrollo antes de aceptarla, los ficheros por su huella digital y las tablas por sus recuentos, y la evaluación del método de referencia coincide con la de desarrollo métrica a métrica.",
      en: "Every piece was checked against development before it was accepted, files by their digest and tables by their counts, and the reference method's evaluation matches development metric by metric.",
      de: "Jedes Teil wurde vor der Übernahme mit der Entwicklungsumgebung abgeglichen, Dateien über ihren Hashwert und Tabellen über ihre Zählungen, und die Auswertung der Referenzmethode stimmt Metrik für Metrik mit der Entwicklungsumgebung überein.",
      pt: "Cada peça foi comparada com o desenvolvimento antes de ser aceite, os ficheiros pela sua impressão digital e as tabelas pelas suas contagens, e a avaliação do método de referência coincide com a do desenvolvimento métrica a métrica.",
      zh: "每个部分在被接受前都与开发环境核对过：文件核对其摘要，数据表核对其计数；参考方法的评估结果与开发环境逐项指标一致。",
    },
  },
  {
    date: "2026-11-24",
    text: {
      es: "Anotar una secuencia vuelve a funcionar: cada predicción lee el grafo GO del conjunto del que toma sus términos.",
      en: "Annotating a sequence works again: each prediction reads the GO graph of the set its terms come from.",
      de: "Das Annotieren einer Sequenz funktioniert wieder: Jede Vorhersage liest den GO-Graphen des Datensatzes, aus dem ihre Begriffe stammen.",
      pt: "Anotar uma sequência volta a funcionar: cada previsão lê o grafo GO do conjunto de onde vêm os seus termos.",
      zh: "序列注释功能已恢复：每个预测都读取其术语来源数据集对应的 GO 图。",
    },
  },
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
