/* 
 * Modelo Promela - Patrón Worker Pool con Local Reduction
 * Curso: CC65 - Programación Concurrente y Distribuida
 * Proyecto: UrbanBus (PC2)
 *
 * Descripción:
 * Este modelo valida la sincronización del algoritmo map-reduce
 * implementado en Go. Modela 3 roles:
 *   1. Producer: Envia N "chunks" de datos y luego señales de fin.
 *   2. Workers (Mappers): Reciben chunks y suman en memoria LOCAL (sin Mutex global).
 *   3. Reducer: Recibe los sub-totales locales y hace el merge final.
 *
 * Invariantes que prueba este modelo:
 * - Absence of Deadlocks: Los canales acotados no se bloquean infinitamente.
 * - Absence of Race Conditions: Los workers solo modifican variables locales.
 * - Correctitud Funcional: La suma final en el Reducer es EXACTAMENTE igual al
 *   total de chunks enviados por el productor (assert final).
 */

#define NUM_WORKERS 3
#define TOTAL_CHUNKS 6
#define EOF_SIGNAL 0
#define DATA_CHUNK 1

/* Canales asíncronos (bufers acotados) */
chan jobs = [3] of { byte };
chan results = [NUM_WORKERS] of { byte };
int final_count = 0; /* Estado global final (sólo modificado por Reducer) */

proctype Producer() {
    byte i = 1;
    do /* 1. Generación de chunks (representa leer el CSV) */
    :: i <= TOTAL_CHUNKS ->
        jobs ! DATA_CHUNK;
        i++;
    :: i > TOTAL_CHUNKS ->
        break;
    od;
    i = 1;
    do /* 2. Señal de terminación (equivalente a close(jobs) en Go) */
    :: i <= NUM_WORKERS ->
        jobs ! EOF_SIGNAL;
        i++;
    :: i > NUM_WORKERS ->
        break;
    od;
}

proctype Worker() {
    byte local_count = 0; /* Memoria local: evita el sync.Mutex */
    byte chunk;
    do
    :: jobs ? chunk ->
        if
        :: chunk == DATA_CHUNK -> /* Operación local */
            local_count = local_count + 1; 
        :: chunk == EOF_SIGNAL ->
            break;
        fi;
    od;
    results ! local_count; /* Envío del resultado parcial*/
}

proctype Reducer() {
    byte i = 1;
    byte partial;
    do /* Espera recibir un resultado por cada worker */
    :: i <= NUM_WORKERS ->
        results ? partial;  
        final_count = final_count + partial; /* Merge final */
        i++;
    :: i > NUM_WORKERS ->
        break;
    od;
    assert(final_count == TOTAL_CHUNKS);
}

/* LTL Properties para Verificación Formal (Observación #3) */

/* 1. Liveness: Eventualmente ( <> ), el procesamiento finalizará con el conteo exacto, demostrando ausencia de Deadlocks. */
ltl eventual_completion { <> (final_count == TOTAL_CHUNKS) }

/* 2. Safety (Absence of Race Condition): Siempre ( [] ), el conteo final nunca excede el total procesado.
   Esto reemplaza una validación trivial de buffers, garantizando matemáticamente que ningún Worker 
   duplica conteos por fallas de concurrencia o de paso de mensajes. */
ltl no_double_counting { [] (final_count <= TOTAL_CHUNKS) }

init {
    atomic {
        run Producer();
        byte i = 1;
        do
        :: i <= NUM_WORKERS -> 
            run Worker(); 
            i++;
        :: i > NUM_WORKERS -> 
            break;
        od;
        run Reducer();
    }
}