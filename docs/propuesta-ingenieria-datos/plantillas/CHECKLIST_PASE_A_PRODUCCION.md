# Checklist de pase a producción

> Se completa **antes** de aprobar el despliegue en Azure DevOps. Quien aprueba
> confirma que revisó esto, no que confía en que alguien lo hizo.
>
> La mayoría de los puntos los verifica el pipeline solo. Los que quedan son los
> que ninguna automatización puede decidir por nosotros.

---

**Versión (tag):** `v__________`
**Fecha y hora de despliegue:** __________
**Solicita:** __________
**Aprueba por Ingeniería de Datos:** __________
**Aprueba por Finanzas:** __________

---

## 1. Lo que el pipeline ya verificó

Si el CI está en verde, estos puntos están cubiertos. Se listan para que quien
aprueba sepa qué **no** tiene que revisar a mano.

- [x] Formato, linting y análisis de secretos
- [x] `catalogo_tablas.yml` y contratos validados contra su esquema
- [x] Bundle válido para los tres targets
- [x] Pruebas unitarias y cobertura sobre el umbral
- [x] Pruebas de integración y de contrato sobre DEV
- [x] Desplegado y verificado en QA

---

## 2. Lo que hay que confirmar a mano

### Certificación

- [ ] El cambio estuvo en QA al menos **un ciclo de carga completo**
- [ ] Finanzas validó el resultado funcional y lo dejó por escrito
- [ ] Si cambia un número contable, se comparó contra MIS y la diferencia está explicada

### Momento

- [ ] El despliegue es **fuera de la ventana de carga** (02:00 – 06:00)
- [ ] No estamos en cierre mensual, o si lo estamos, hay autorización expresa de gerencia
- [ ] Hay alguien disponible durante la siguiente carga por si algo sale mal

### Reversión

- [ ] El tag anterior está identificado: `v__________`
- [ ] Sé qué tablas reescribe este cambio y a qué versión habría que restaurarlas
- [ ] La retención de time travel cubre el período que haría falta

### Comunicación

- [ ] Finanzas sabe que se despliega y qué cambia
- [ ] Si afecta a un tablero, los usuarios de ese tablero están avisados
- [ ] La guardia sabe qué se desplegó y qué mirar

---

## 3. Después del despliegue

- [ ] La verificación posterior del pipeline pasó en verde
- [ ] Los trabajos están habilitados y con la programación correcta
- [ ] Se observó la **primera carga completa** posterior al cambio
- [ ] Los tableros afectados refrescaron y muestran la fecha correcta del dato
- [ ] El cuadre contra MIS del primer día está dentro de tolerancia

---

## 4. Si algo salió mal

No improvisar. El orden es este:

1. **Primera pregunta:** ¿el error está en nuestro código o en el dato de origen?
   Si es del origen, revertir es lo equivocado: devolvería la tabla a un estado
   que tampoco era correcto.
2. Revertir el código desplegando el tag anterior.
3. Si hubo escritura de datos, revertir **de Gold hacia atrás**: Gold es lo que ve
   el usuario y es lo que hay que estabilizar primero.
4. Avisar a Finanzas antes de que lo noten en el tablero.
5. Análisis posterior sin buscar culpables, dentro de las 48 horas.

Runbook completo en `docs/runbooks/`.

---

## Firmas

| Rol | Nombre | Fecha | Conforme |
|---|---|---|---|
| Ingeniería de Datos | | | |
| Finanzas | | | |
| Arquitectura *(solo si toca Gold Enterprise)* | | | |
