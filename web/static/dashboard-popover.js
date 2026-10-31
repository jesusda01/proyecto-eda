// Educational animations are schematic; execution samples come from Python.
(() => {
  const text = (x, y, label, extra = "") =>
    `<text x="${x}" y="${y}" ${extra}>${label}</text>`;
  const box = (x, y, label, cls = "") =>
    `<g class="${cls}"><rect x="${x}" y="${y}" width="58" height="32" rx="7"/>${text(x + 29, y + 20, label, 'text-anchor="middle"')}</g>`;
  const line = (x, y, a, b) => `<path d="M ${x} ${y} L ${a} ${b}"/>`;
  const dot = (x, y, label, cls = "") =>
    `<g class="${cls}"><circle cx="${x}" cy="${y}" r="19"/>${text(x, y + 4, label, 'text-anchor="middle"')}</g>`;
  const flow = (path) =>
    `<circle r="4" class="bright"><animateMotion dur="2.5s" repeatCount="indefinite" path="${path}"/></circle>`;
  const info = [
    [
      "El primero que entra es el primero que sale.",
      "Cada evento se añade al final de la cola. Al formar un lote, se extraen los más antiguos desde el principio.",
      "Encolar y extraer: O(1).",
      `${text(24, 23, "ENTRADA")}${text(265, 23, "SALIDA")}${line(20, 75, 330, 75)}${box(44, 59, "C", "queue-last")}${box(118, 59, "B")}${box(192, 59, "A", "queue-first")}${flow("M 20 75 L 330 75")}${text(68, 139, "C llega después · A sale primero")}`,
    ],
    [
      "Quitar repeticiones y ordenar por fecha.",
      "Compara los IDs con el histórico y con los del lote. Conserva un evento por ID y luego aplica sorted() por event_time.",
      "Timsort: O(n log n) en el peor caso. El primer lote deduplica con una lista.",
      `${box(18, 30, "A")}${box(98, 30, "B")}${box(178, 30, "A", "duplicate")}${box(258, 30, "C")}${line(170, 73, 170, 99)}${box(58, 108, "A")}${box(138, 108, "B")}${box(218, 108, "C")}${text(19, 22, "ID REPETIDO → DESCARTAR")}`,
    ],
    [
      "La prioridad menor se atiende primero.",
      "El heap coloca la prioridad mínima en la raíz. Las alertas tienen prioridad 0; las compras respetan express, estándar y programada. Los empates usan fecha y orden de llegada.",
      "Insertar y extraer: O(log n).",
      `${line(170, 47, 90, 116)}${line(170, 47, 250, 116)}${dot(170, 45, "0", "pulse")}${dot(90, 119, "2")}${dot(250, 119, "6")}${text(214, 43, "SALE PRIMERO")}${flow("M 170 45 L 170 8")}`,
    ],
    [
      "Consultar por ID y recordar lo reciente.",
      "La primera consulta busca el evento en el diccionario y lo añade a la caché: MISS. La siguiente lo encuentra en caché: HIT. Si se llena, sale el menos reciente.",
      "Lookup y operaciones LRU: O(1) esperado.",
      `${box(17, 60, "ID")}${box(141, 24, "LRU", "pulse")}${box(141, 107, "HASH")}${line(75, 76, 141, 40)}${line(75, 76, 141, 123)}${text(222, 43, "HIT → rápido")}${text(222, 127, "MISS → guardar")}${flow("M 75 76 L 141 40 L 199 40 L 316 40")}`,
    ],
    [
      "Distribuir claves en un árbol ordenado.",
      "Las claves se insertan en hojas. Cuando un nodo se llena, se divide y una clave separadora asciende. El dibujo explica la idea del B+; no reconstruye el árbol real.",
      "El B+ consulta rangos y persiste páginas. Al reabrir el archivo conserva sus claves y valores.",
      `${box(141, 15, "50")}${line(170, 47, 90, 95)}${line(170, 47, 250, 95)}${box(42, 99, "10 · 30")}${box(224, 99, "50 · 80", "pulse")}${line(100, 115, 224, 115)}${text(95, 158, "Hojas ordenadas y enlazadas")}`,
    ],
    [
      "Medir qué puntos están más cerca.",
      "Se construye un KD-Tree con precio y user_id. La búsqueda utilizada es la línea base exacta: calcula todas las distancias y las ordena; no consulta el árbol.",
      "La consulta actual cuesta O(n log n), por la ordenación final.",
      `${line(32, 140, 320, 140)}${line(32, 140, 32, 18)}${text(280, 158, "precio")}${text(40, 22, "atributo")}${[
        [83, 99],
        [130, 47],
        [211, 92],
        [273, 41],
      ]
        .map(
          ([x, y]) =>
            `${line(174, 112, x, y)}<circle cx="${x}" cy="${y}" r="5"/>`,
        )
        .join(
          "",
        )}<circle cx="174" cy="112" r="6" class="bright"/><circle cx="174" cy="112" r="30" class="scan"/>${text(181, 132, "consulta")}`,
    ],
    [
      "Resumir pertenencia y frecuencia.",
      "Bloom activa posiciones con hashes: un cero significa que no se observó; todos unos indican presencia probable. Count-Min suma contadores y devuelve el mínimo de sus filas.",
      "Bloom puede dar falsos positivos; Count-Min puede sobreestimar.",
      `${box(12, 63, "ID")}${[0, 1, 2].map((i) => `${line(70, 79, 145, 35 + i * 45)}${text(93, 31 + i * 45, "hash " + i)}${box(145, 19 + i * 45, "1", "hash-bit")}${text(224, 40 + i * 45, "contador +1")}`).join("")}${flow("M 70 79 L 145 35")}`,
    ],
    [
      "Comparar canastas y vectores.",
      "MinHash resume los productos de un usuario en una firma y estima Jaccard comparando coincidencias. La búsqueda vectorial actual calcula todas las distancias; la clase no implementa HNSW real ni hay índice LSH.",
      "La animación representa la comparación de firmas.",
      `${text(13, 34, "USUARIO A")}${text(13, 105, "USUARIO B")}${[7, 2, 9].map((v, i) => box(107 + i * 73, 15, String(v), i === 1 ? "pulse" : "")).join("")}${[4, 2, 8].map((v, i) => box(107 + i * 73, 86, String(v), i === 1 ? "pulse" : "")).join("")}${line(209, 47, 209, 86)}${text(95, 153, "1 coincidencia / 3 posiciones")}`,
    ],
    [
      "Relacionar usuarios con productos.",
      "Cada interacción crea una arista usuario → producto. PageRank puntúa los nodos y modularidad voraz agrupa comunidades. Las interacciones repetidas no acumulan peso en esta demo.",
      "El dibujo muestra relaciones; no es el grafo completo de la ejecución.",
      `${line(65, 42, 172, 85)}${line(65, 133, 172, 85)}${line(172, 85, 278, 42)}${line(172, 85, 278, 133)}${dot(65, 42, "P1")}${dot(65, 133, "P2")}${dot(172, 85, "P", "pulse")}${dot(278, 42, "P3")}${dot(278, 133, "P4")}${flow("M 65 42 L 172 85")}${flow("M 278 133 L 172 85")}`,
    ],
    [
      "Completar prefijos y ordenar popularidad.",
      "El Trie recorre caracteres del prefijo y busca sus continuaciones. Top-K cuenta interacciones por producto y selecciona los tres con más apariciones.",
      "Las barras ilustran frecuencias, no valores del dataset.",
      `${dot(43, 33, "p")}${line(62, 33, 93, 33)}${dot(113, 33, "r")}${line(132, 33, 163, 33)}${dot(183, 33, "o", "pulse")}${text(231, 36, "→ producto…")}${[180, 125, 70].map((w, i) => `<rect class="rank-bar" x="89" y="${76 + i * 29}" width="${w}" height="17" rx="4" style="animation-delay:${i * 0.25}s"/>${text(23, 89 + i * 29, "#" + (i + 1))}`).join("")}`,
    ],
  ];

  const popup = document.createElement("section");
  popup.className = "algorithm-popover";
  popup.hidden = true;
  popup.setAttribute("role", "dialog");
  popup.setAttribute("aria-labelledby", "algorithm-title");
  popup.id = "algorithm-popover";
  popup.innerHTML = `<div class="popover-top"><span class="eyebrow" id="algorithm-step"></span><div><button id="algorithm-pin" title="Mantener abierta">Fijar</button><button id="algorithm-close" aria-label="Cerrar explicación">×</button></div></div><h2 id="algorithm-title"></h2><p class="algorithm-lead"></p><div class="algorithm-animation"></div><div class="animation-caption">Animación explicativa · esquema del algoritmo</div><p class="algorithm-explanation"></p><p class="algorithm-cost"></p><div class="algorithm-live"><span>DATOS DE ESTA EJECUCIÓN</span><pre></pre></div>`;
  document.body.append(popup);
  let current = null,
    pinned = false,
    openTimer,
    closeTimer;
  const pin = popup.querySelector("#algorithm-pin");

  function refreshData() {
    if (current === null || popup.hidden) return;
    const stage = state.stages[current];
    const source = stage?.data || stage?.input;
    const keys = [
      ["lote_crudo", "pendientes_buffer"],
      ["lote_ordenado", "rechazados", "duplicados"],
      ["lote_priorizado", "orden_prioridades"],
      ["almacenados", "status1", "status2"],
      ["productos_catalogo", "consulta_rango", "indice_disco"],
      ["knn_res", "referencia_exacta", "kd_coincide"],
      ["alertas", "decisiones", "ventana_segundos"],
      ["neighbors_hnsw", "recall_at_k", "usuarios_similares", "capas_hnsw"],
      ["grafo", "top_nodes", "comunidades"],
      ["recomendaciones", "top_k", "alertas", "contadores"],
    ][current];
    const sample = {};
    for (const key of keys) {
      if (source?.[key] !== undefined) {
        const value = source[key];
        sample[key] =
          value?.cantidad !== undefined
            ? { cantidad: value.cantidad, muestra: value.muestra?.slice(0, 1) }
            : value;
      }
    }
    popup.querySelector(".algorithm-live pre").textContent = Object.keys(sample)
      .length
      ? JSON.stringify(sample, null, 2)
      : "Disponible cuando esta etapa reciba datos. La animación ya puede explorarse.";
  }

  function position() {
    if (current === null || popup.hidden) return;
    const anchor = nodes.children[current].getBoundingClientRect();
    const width = popup.offsetWidth,
      height = popup.offsetHeight,
      pad = 12;
    let left = anchor.right + 16;
    if (left + width > innerWidth - pad) left = anchor.left - width - 16;
    if (left < pad) left = Math.max(pad, (innerWidth - width) / 2);
    const top = Math.max(
      pad,
      Math.min(anchor.top - 25, innerHeight - height - pad),
    );
    popup.style.left = left + "px";
    popup.style.top = top + "px";
  }

  function show(index, fix = false) {
    clearTimeout(closeTimer);
    clearTimeout(openTimer);
    if (pinned && !fix) return;
    const changed = current !== index;
    current = index;
    pinned = fix || pinned;
    pin.textContent = pinned ? "Fijada" : "Fijar";
    pin.setAttribute("aria-pressed", String(pinned));
    if (changed) {
      popup.querySelector("#algorithm-step").textContent =
        "ETAPA " + String(index).padStart(2, "0") + " · " + stages[index][1];
      popup.querySelector("#algorithm-title").textContent = stages[index][0];
      popup.querySelector(".algorithm-lead").textContent = info[index][0];
      popup.querySelector(".algorithm-explanation").textContent =
        info[index][1];
      popup.querySelector(".algorithm-cost").textContent = info[index][2];
      popup.querySelector(".algorithm-animation").innerHTML =
        `<svg viewBox="0 0 345 175" role="img" aria-label="Esquema animado de ${stages[index][0]}">${info[index][3]}</svg>`;
      // Respect reduced motion for SVG animations as well as CSS.
      if (matchMedia("(prefers-reduced-motion: reduce)").matches)
        popup.querySelector("svg").pauseAnimations();
    }
    popup.hidden = false;
    refreshData();
    position();
  }

  function hide() {
    clearTimeout(openTimer);
    clearTimeout(closeTimer);
    const restoreFocus = popup.contains(document.activeElement),
      previous = current;
    popup.hidden = true;
    current = null;
    pinned = false;
    if (restoreFocus && previous !== null) {
      nodes.children[previous].focus();
      clearTimeout(openTimer);
    }
  }

  function scheduleClose() {
    if (!pinned)
      closeTimer = setTimeout(() => {
        if (!popup.matches(":hover") && !popup.contains(document.activeElement))
          hide();
      }, 220);
  }
  [...nodes.children].forEach((node, index) => {
    node.removeAttribute("title");
    node.setAttribute("aria-controls", popup.id);
    node.setAttribute("aria-haspopup", "dialog");
    node.addEventListener("pointerenter", () => {
      if (matchMedia("(hover: hover)").matches) {
        clearTimeout(closeTimer);
        openTimer = setTimeout(() => show(index), 180);
      }
    });
    node.addEventListener("pointerleave", () => {
      clearTimeout(openTimer);
      scheduleClose();
    });
    node.addEventListener("focus", () => {
      openTimer = setTimeout(() => show(index), 180);
    });
    node.addEventListener("blur", scheduleClose);
    node.addEventListener("click", () => show(index, true));
  });
  popup.addEventListener("pointerenter", () => clearTimeout(closeTimer));
  popup.addEventListener("pointerleave", scheduleClose);
  popup.addEventListener("focusout", scheduleClose);
  pin.onclick = () => {
    pinned = !pinned;
    pin.textContent = pinned ? "Fijada" : "Fijar";
    pin.setAttribute("aria-pressed", String(pinned));
  };
  popup.querySelector("#algorithm-close").onclick = hide;
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && !popup.hidden) {
      event.preventDefault();
      hide();
    }
  });
  document.addEventListener("pointerdown", (event) => {
    if (
      !popup.hidden &&
      !popup.contains(event.target) &&
      !event.target.closest(".node")
    )
      hide();
  });
  addEventListener("resize", position);
  setInterval(refreshData, 400);
})();
