const Rx = require('rxjs');
const Op = require('rxjs/operators');

const express = require('express')
const app = express()
const port = 3000

let ecs_subject$ = new Rx.Subject();
let ps_subject$ = new Rx.Subject();

let ps_occupation = -1;
let ps_extension = 0;
let ps_validTime ;
let system_status = 'Unknown';

app.use(express.json());
app.set('view engine', 'pug')

app.listen(port, () => {
    console.log(`GUI address at http://localhost:${port}`)
})

app.get('/', function (req, res) {
    res.render('index', {
        title: 'Simple GUI',
        message: 'Parking Station extension',
        var1: ps_occupation,
        validTime : ps_validTime,
        var2: ps_extension,
        ss: system_status})
})

app.put('/ps_measurements',(req,res) =>
{
    console.log("Caught a ps web-hook. Emitting values !")
    for ( let measurement of req.body){
        ps_subject$.next(measurement)
    }
    res.send('OK')
})

app.put('/ecs_measurements',(req, res) =>
{
    console.log("Caught an ecs web-hook. Emitting values !")
    for ( let measurement of req.body){
        ecs_subject$.next(measurement)
    }
    res.send('OK');
})

const ps_observable$ = ps_subject$.pipe(
    Op.map(measurement => measurement._source),
    Op.filter(measurement => measurement.code === '104'));

const ecs_observable1$ = ecs_subject$.pipe(
    Op.map(measurement => measurement._source),
    Op.filter(measurement => measurement.code === 'IT*ETN*E00411'));

const ecs_observable2$ = ecs_subject$.pipe(
    Op.map(measurement => measurement._source),
    Op.filter(measurement => measurement.code === 'IT*ETN*E00412'));

const ecs_observable3$ = ecs_subject$.pipe(
    Op.map(measurement => measurement._source),
    Op.filter(measurement => measurement.code === 'ASM_00000274')
);

const ecs_value_observable1$ = ecs_observable1$.pipe(Op.map(measurement => measurement.value));

const ecs_value_observable2$ = ecs_observable2$.pipe(Op.map(measurement => measurement.value));

const ecs_value_observable3$ = ecs_observable3$.pipe(Op.map(measurement => measurement.value));

const esc_combined_observable$ = ecs_value_observable1$.pipe(
    Op.combineLatestWith(ecs_value_observable2$),
    Op.map(([esc1, esc2]) => esc1 + esc2 ))
    .pipe(
        Op.combineLatestWith(ecs_value_observable3$),
        Op.map(([esc12, esc3]) => esc12 + esc3 - 1));

const ps_log_observer = ps_observable$.subscribe(get_ps_occupation);
const ecs_log_observer = esc_combined_observable$.subscribe(get_ps_extension);

const system_status_interval$ =  Rx.interval(1000*60)


const ecs_ts_observable1$ = ecs_observable1$.pipe(Op.map(measurement => checkValidTime(measurement.validTime)));
const ecs_ts_observable2$ = ecs_observable2$.pipe(Op.map(measurement => checkValidTime(measurement.validTime)));
const ecs_ts_observable3$ = ecs_observable3$.pipe(Op.map(measurement => checkValidTime(measurement.validTime)));
const ps_ts_observable$ = ps_observable$.pipe(Op.map(measurement => checkValidTime(measurement.validTime)));

const system_status_observable$ = ecs_ts_observable1$
    .pipe(
        Op.combineLatestWith(ecs_ts_observable2$),
        Op.map(([esc1, esc2]) => esc1 + esc2 ))
    .pipe(
        Op.combineLatestWith(ecs_ts_observable3$),
        Op.map(([esc12, esc3]) => esc12 + esc3))
    .pipe(
        Op.combineLatestWith(ps_ts_observable$),
        Op.map(([esc123, ps]) => esc123 * ps))

const system_status_checker$ = system_status_interval$.pipe(
    Op.zipWith(system_status_observable$),
    Op.map((_,val) => val))

const system_status_observer = system_status_checker$.subscribe(get_system_status);

function checkValidTime(validTime){
    let diff = new Date() - new Date(validTime);
    if (Math.floor((diff/1000)/60) <= 10){
        return 1
    }else{
        return 0
    }}


function logFunction (measurement) {
    console.log("Subscriber logging ...")
    console.log(measurement)
}

function get_ps_extension(measurement) {
    ps_extension = measurement
}
function get_ps_occupation(measurement) {
    ps_occupation = measurement.value;
    ps_validTime = new Date(measurement.validTime);
}

function get_system_status(value){
    switch(value) {
        case 0:
            system_status = 'Red'
            break;
        case 3:
            system_status = 'Green'
            break;
        default:
            system_status = 'Orange'
    }

}