
- How Kafka Works?
- Why Producers Writes are Slow
- How Allegro Traced the Kafka Protocol 
- How Tracing Kernel System Calls
- What is a Journaled File Systems
- How Allegro Tuned ext4
- And finally Switching to XFS

On Linux, Kafka uses regular files for storing data. Writes are done using ordinary write system calls — data is first stored in the page cache and then asynchronously flushed to disk"
"Let’s go back to ext4. We knew that journal commits were the source of latency". Hence the choice for XFS which seemingly handles the process better.

x amount of data copied from a socket to userspace memory, then from userspace to page cache memory, then page cache to disk vs direct socket to disk is the true power of zero copy (kafka uses it).

Another big problem with ext4 is its slow and non-tunable allocation algorithm. Prefer burst of large pre-allocation then write data, it will be way more faster.

If kernel TLS is enabled then SSL_sendfile() can be used (userspace encryption may be performant but has a higher chance of not using the right cpu instructions to accelerate the decryption). I hardly doubt though that somebody made the patch for SSL_sendfile() in kafka. Nginx uses it like a pro. In small organisation kafka is exposed inside a secure network and tls is disabled for performance. TBH kafka is not supposed to be external client facing. Also zero security is not practiced inside small organisations. 




this company have improved cfa's

producers rights by over

82% by switching file

systems but the way they got there is

beautifully written in this technical

blog that I want to share with you in

this episode of the back Eng Jing show

how about we jump into it so this

company is called algro the title of

this blog is unlocking cfast potential

by tackling tail latency with

ebpf not the best clickbait title if you

ask me but the content is beautiful you

know the way they went into details of

showing them you know showing us how

they got there by using out of the box

tooling you know they rarely you know go

go and recompile source code also kka is

open source you know they could have

recompiled and and did all this stuff

but no they used out of the box tooling

which I love you know this is a way to

look transparently into things they

traced Kafka they parsed the protocols

of TKA the TCP binary protocol they use

the protocol the the pro binary protocol

that kfka use on top of TCP TCP is it

own it's a protocol but Kafka uses its

own binary on top of that they passed

that to find out

the request and the slow request and

responses then they match that with the

kernel they trace the kernel to find out

what is the corresponding what happened

and they found out that there is a

Slowdown in the file system which is

called

ext4 know then they dove into the kernel

of all these calls the file system calls

and they found exactly where the problem

is spoiler is it was a lock when it

comes to a commit to those file systems

they eventually did uh some certain

configuration to the file system to

improve those latencies and they

eventually decided to switch the file

system Al together to a new file system

called xfs which gave them this

beautiful boost 82% now take so they

didn't go all the way and explain why

xfs is faster in that particular case

than ext4 so take that word result this

does not mean ext4 is bad it's just it

depends on the use cases so let's dive

into

this Kafka we all know Kafka do we I in

a video years ago this that video needs

a desperate refresh cfco is this

beautiful publish subscribe system you

it's allowed for asynchronous

consumption yeah this is Kafka so this

is it is the uh asynchronous way of

communication it's one of the design

pattern I like to explain and in my

backend course where usually when you

send a request like how do how do you

consume stuff you know from two

different party I have something to give

you I have something to to read you know

there's two entities they need to read

from each other how do you do that you

can let them talk to each other but they

will be completely out of sync you know

some times I have data to publish I have

some to send but the client is not ready

to consume you know they're this is not

always one to one so people invented

this idea it's like all right let's take

your data publish it somewhere and then

forget about it that's it forget about

consum who's consuming it who's going to

read it forget about it just move on

right so you you're publishing to

something called a broker and then and

and or producing that's the word us it

and then there's a consumer on the other

end

that that that you know on its Pace on

its own pace consumes those you know the

content that you publish you know and

there is so many things you can do here

you know how to you consume it do you

want it pushed do you want long PLL do

you want to short PLL it do you just do

it on on demand whenever I feel like it

I'm going to request I'm not interested

in latest update and and all of this

stuff is I explained so that's kavka you

publish you produce so you write to that

Brokers there's a right so that gives

back to this we're writing to this

beautiful broker and this is where

things get slower yeah and I'm going to

I'm not going to read every single word

here because first of all it's going to

take me being slow it's going to take me

seven hours to read second I really need

to give credit to those guys you read to

go visit this page because if I read the

whole thing then you there is no reason

for you to check the blog isn't

right mass and I apologize I cannot read

the names I'll try my best uh the

authors always CRI the authors mass

m these are European names of

course P

Rosco I apologize from the bottom of my

koni okay so I produce request and they

saw that these request

are suffering you know these requests

are being suffered look at this look at

all this stuff what is this what is

this 3

seconds yiki 3 seconds to

write to produce to the C to the Kafka

broker you know this is not consumption

consumption is read only right I mean

there is a little bit of a ride when you

consum you you move the position of

where you're writing so there's a tiny

write but there's nothing really a

problem when it comes to reading so

there's no problem with reading there's

the most problem with writing right

that's the the they that they faced

right of course they they can be read

problems but they did not see it they

didn't talk about it here so we're

writing we're seeing at p999 I talk

about these latencies but yeah so they

found that the more of course every time

you add a nine you're you're picking up

the slower request you know so this this

999 is three and two 9es I think it's 1

second which is this somewhere over here

and I have a video explaining these

latencies like 999 if yeah so you have

three

seconds and this is slow of course right

in the worst case scenario so what there

so that's that's the problem we know the

problem

well they they just know that the

producer is slow they don't know what

requ

they have no idea zero transparency from

Kafka the first thing they think all

right we need to trace Kafka and guess

what I was surprised there is no metrix

when it comes to Kafka that

captures hey this we sent a request this

is we logging the request I'm I'm pretty

sure there is they might have missed

this because I'll be surprised to see

that there is no logging when it comes

comes to Kafka when it comes to hey

someone just made a producer request to

this message to this topic to this

partition and it took this much and it

arrives there is nothing like that

apparently they had to roll their own

and this is March

2024 so maybe there something like that

doesn't exist is Kafka that fast that

nobody actually decided to write

something like that I'm surprised you

Kafka experts let me know in the comment

section below so we have a

Slowdown we have slowdown so they say

hey let's write a tool that parses the

protocol I love this I absolutely love

this look at this look at this they said

all

right well the kfka is basically built

on TCP you don't really get much options

really right either TCP or UDP to be

honest I don't get much options so so

it's a b out protocol it's it's own

protocol and what did I say what do I

keep saying about uh requests and

responses you know the the concept of a

request and response doesn't exist in

TCP it's a higher level application

Level in this case Kafka so you need to

understand what a request is and what a

response is and they had to parse that

they have to read these segments read

read and start parsing the protocols oh

we we just got something producer so

they put this code on the broker side

right oh just got a request right so

they had they built a parser

essentially and then they captured all

this information which message and now

we have a request you you don't know how

long the request took you just said oh I

got the request so using TCB which a

tool I used many times in this

channel H also wire shark is a beautiful

other tool you can use but you they they

they waited so they have they have they

passed this they know a request but then

to to know how long a request took you

need to wait for the response which is

basically the commit says

hey I got your response whatever that I

think we send back the position uh or or

like an acknowledgement say

acknowledgement we position the consumer

the producer will just send an act act

got it so they got it thankfully that

resp response has also the message ID

and the topic so they made they was able

to uh you know link the request with the

response and just like that you can uh

they they got the arri time and they got

the end time too okay so you have this

particular thing of course there is

additional time for processing the

request itself okay so this tells me

that this they did this this without

using TLS because if they are TLS they

are completely blind right I suppose

they have to decrypt it which

is I suppose you can do CU we' showned

that in this channel how to decrypt TLS

traffic using curl at least uh by by by

setting a specific environment variable

and you can decrypt it cuz you own

everything essentially and you tell the

tool hey tool write your keys and you

the symmetric keys and let the wire

shark read from those keys and you can

decrypt it but for this they had to

disable TLS because it's okay you can

disable TS I'm just I'm just uh uh you

know what was called the tracing I'm I'm

doing monitoring disabled TLS who cares

nobody's consuming my system right now

but they better hopefully enabled TLS

after all of this stuff right especially

if this whole stuff is in the cloud all

right so we now have requests we know

the message that written the topic the

partition and they loan the latencies

it's 500 see 500 500 this is just

example 500 millisecond 500 milliseconds

5 millisecond so so we know they found a

way to find all the slow

request that's all what they got now

well by

knowing the slow

requests what

we we need to know where is the time

being spent now we know that the produce

is just as simply like he spends a

little bit of time on kfka but the

almost time they just writing so that's

what they did next they

traced dynamically using

ebpf you know Berkeley file system uh

Berkeley packet

filter the right

calls because I said all right Kafka is

not doing much we're just writing to the

to the file system so let's capture the

right calls to the file system that's

what they did so they started um they

started tracing Kafka with this approach

right and they found that when they

started capturing this now they have

remember we have the request which is a

blind a request that took x amount of

time but they don't know how do they

know all of this

Fleet of right system file system calls

to the file system how do they know that

which right corresponds to which request

that is a really challenging thing to do

and they did it

brilliantly so they traced this ext4

because that's the file system right

using this ext4 slower uh uh epbf

extension know and they managed

to find the right file system right

calls which which is that how you write

to the file system captured it and they

know the

file but if you know the file you know

the Parent Directory and guess what the

Parent Directory in Kafka is the topic

the file is what the message ID the

partition yes the file the file the

segment file is the partition itself you

know so we have topic and we know the

partition so they part using the file

name and The Parent Directory they know

exactly and which thread made that

request which is very important because

that's another thing it's a

multi-threaded system so you having

multiple threads competing on the same

file that's a recipe for you know con

contention but now they have all this

information they partition ID topic name

and here's here's what they built they

linked it back to the request because if

you know the topic you know the message

you know all of the stuff

you can bring all of this together into

a single beautiful table now they know

the request they know the latency they

know the message they know the topic

they know the partition and they know

the latency okay we know everything now

everything is there and all they s is

like all right so we can correspond how

long a right call talk right a file

system right call Talk versus is the how

long the request took and if the request

is equal to the amount of time to write

to the file system or eventually to desk

then that whole thing that whole request

is just spent writing to disk that's

cool and they found a lot of requests

like that the entire time processing the

request is just right which is this

whatever 50

millisecond what

15 but then they saw slow rights

some slow rights some

requests that had they are slow request

but with a faster rights it's like

what

how there are slow request but with a

faster r that means there is a bunch of

space and if you're listening to this

we're showing a trffic where there is a

a whole request from request to end that

is a huge but then there is a tiny box

that says okay the file system talk only

this much but there is a big question

mark we have no idea what happened

before that and you might guess what it

they figured it out it's a lock right

and I'm not going to go into detail what

that kind lock is actually you know that

knowing me I of course I have to go into

the details but it's essentially waiting

for a

lock or waiting for a permission to ride

to the fire so before we actually did

the ride we did something we just waited

this thread that try to attempt to do

the ride to producing their ride it was

just waiting was blocked why because

well the file system this comes back to

the file system you guys file system

and I talk about this in my new

operating system it's really if you kind

of dive deep into it it has

metadata right because we decided that

file people need to work with files in

directory

right we decided that because we like

directories and files and well what is

that this is all meta data because there

is the data you write I won't write

blocks of data but then there is the

ability to track the size of the file

and the permission of the file and this

file lives in this directory and this

file this directory has these files and

this file all this

metadata belong to the file system that

is not really user

data well it is you might know what I'm

saying but is if I write my produce

content like I sent I don't know

adjacent file

I don't really care if it's in this file

or this directory yet it's it's an

what's it's a WR

amplification side effect right you

decided to add this

metadata just to write my stuff right

it's on you and this metadata has a

cost and all of this stuff is in

something called an iode an index node

and every file system implements this

slightly

differently now this is where a file

system behaves almost identically to a

database

well file system directories and files

really need to be consistent if I if I

no two threads can delete one thread can

rename a file and other can delete it

you cannot do that or one can change the

content and the one can move it to

another directly you need to stay

consistent and the moment you introduce

this stuff you need to journal it you

need a wall right ahead log and that's

what almost all file systems are

journaled as is that all changes that

you are making first are written to a

different St called the journal is hey

someone on this date made this change

the one this me this change to this file

that's it and then there's the whole

thing which is the actual content and

the blocks of the data right we don't

write directly to those exactly like

with the database we're doing well in

the database we write to the wall and

then we write to the pages in memory and

then we it's safe we say all right we

can flush safely the journal while the

pages are in memory also up to date of

course you have to write to them right

but if we crash the pages on disc are

different but that's fine if we came

back I'll replay the journal is it reply

or replay replay I always confus this

reply Replay play to play it we want to

replay it we want to replay all of these

things back to the old last checkpoint

that's exactly how file systems work

right to stay consistent so that's

journaling and and you cannot start

journaling from two different thread on

the same thing if a transaction and they

call it also transaction if someone is

doing some changes on a file the other

threads are especially when you're

journaling when you're committing

stuff and that is basically where most

of the time is spent you can go into the

details of which uh kernel functions are

actually doing the Locking and that's

how ext4 is essentially implemented and

this is the function it's called Dirty I

know so they did all sorts of monitoring

they all right what can we do what can

we make how can we make make this faster

you guys how can we make this you know

more Haku how how can we make it

faster

well you can disable

journaling that's one option disable all

together you can you

can can you do that in

databases you can in databases we tell

you to disable fsync so that we we have

to journal but we don't force you to to

sink those walls back to dis it says H

it's all right it's all right if you

don't so that's one option they said

nope that's dangerous if you don't

Journal you are corrupt basically right

what they what they said here without

journaling we we would risk long

recovery in case of a crash I don't know

if that's true I think you're straight

up corrupted right I don't know how you

not journal and be consistent I don't

think it's

possible cuz in case of a crash while

rewriting this beautiful block of data

let's say you wrote and that's how you

know the LBA is presented by the desk

all right you might write one file

system block 4 kilobyte but it's

segmented into eight

lbas right so you you're writing eight

lbas and Depends over in the in the

physical uh logical physical sector size

how big that is compared to you block

size that if that's out of s you're

screwed you get you're anatomic in case

of a crash of course some people says

hey I live on the edge we live on the

edge it's

okay we can

decrease so that's our question decrease

the commit interval let's decrease it

right how often we are flushing the wall

or the journal it's like all right uh

the default is 5 Second Every 5 Second

let's flush it right if you don't flush

the kernel the journal you are risking

what you're risking longer recovery CU

you're accumulating all this beautiful

world changes in your memory and in case

of a of a crash basically you lost these

changes alt together I think

right so how often you want to commit

these changes is up to you like it's

every 5 Seconds you can reduce it the if

you reduce it you have less changes to

write of course and as a result you of

course you're writing more but you're

writing less I mean you're writing a

little bit less so you can if you

increase it you're not going to dis as

often and as a result it improves the

performance they said nah didn't help

much they said all right let's change

the journaling mode all together and

instead of doing an order right that is

let's write to the metadata and then and

then flush the content itself well let's

do it a write back that's another

approach write back means what is right

back that's right write back it

guarantees that the data is written to

the main file system so when I do a an

actual file write I don't care about the

metadata of the file write my content

later flush your metadata file system I

don't care about that okay and there is

an actual option in at least post gr

called if data sync like well it's an a

system call but you can switch it such

that hey sync the data not if sync if

sync means sync

everything to desk right anything you

have in the page cach cuz all everything

what we're talking about is almost in

the page cach right some of it is the

and the file

system me uh dedicated place for memory

but then yeah you're doing all this

stuff let me write my data that's what I

care about your file systems stuff I

don't care about write it later but

you're of course there's always a risk

involved with all this stuff but how

often all this crashes some people take

their care uh chances and say all right

let's let's lose some data and some of

these stuff are repairable and operating

systems all file systems come with a

repair mechanism all right so they they

did Le change they did a right back and

they improve from 3 seconds all the way

to 800 millisecond so the right back

improved so the F data sync is actually

very good so you can do fast comments

that's another approach right if you

switch to fast comments it's

a it's a better approach to committing

those file system changes right so they

they saw some little bit of better

approach there is a little bit of better

performance with fast commit they did

that so I just like this journey through

the blog it's like hey we're making it

better better so they reduced it all the

way to 500 millisecond but look at that

they still their average seems like to

be in in the 250 right that's their

average right but now their tail latency

is now way down which is good all right

so the

finally they decided to switch all all

file systems all together and here is

where I'm I'm waiting for a part two of

this blog because they switch file

system and because kfka really writes

huge append only large files they found

that xfs is

better why I absolutely have no

idea right I didn't dive into the

intricacies of xfs or X xd4 to be honest

right this is something I want to do you

know this

really they could be you can write a

whole book about how xfs work or how xt4

work cuz there are just so much more you

know I'm I talk about this in an

overview here as a you know as a podcast

but there's so much so much here so they

switched to xfs and look at the

difference

here all of these you know tail

latencies of course they didn't they

show the base right they didn't show the

changes you no with fast commit that's

the latest

thing with xfs they still saw some

tail but it caps at 500 milliseconds

just like the fast Comming but it's

way better you see it's almost an

average almost for those listening we're

seeing uh what 20 millisecond average

maybe perhaps

less you know so this is a a video I

wanted to make to go into you know the

details of how you can

elegantly trace the

engineering

art of having a problem and slowly just

sculpturing it slowly just you know

going through every single

thing and getting to the bottom of it

and they did a good job they switched to

xfs now I don't think xfs has no

problems

we're going to hear back from those guys

later maybe I don't know in a year or

two and we always do with with these

things like oh we switch and it's so

much faster then they come two years

later and says

actually we fixed this and that broke we

fixed that and that yeah it's always the

case like look just look at Discord like

they move from Mingo mango DB to

what to Cassandra and then to Sila

May right because every move there was a

problem to them they saw something and

then something else uh pops up right

especially with you as you become also

popular you get more load workload and

as a result you get more uh no problems

More Money More Problems as they say

right so shout out

to them guys

great uh Mass MOSI

Miki and then

P

Risco you guys that's a fantastic blog I

I can encourage you to read it you know

and uh not only read it read the links

you guys read the links they they

provide a beautiful beautiful you know

resources you know all these link they

say hey we found this tool it's called

async profiler we found this Tool uh I

don't know

xt4 whatever you know slower that's the

epbf and we wrote this and we wrote that

I really hope they they Upstream their

changes to kfka know how did they you

know capture the request all the way to

the false system this is really I I

don't think it's like a one tool like

they use many tools and to link this

together it's like it's going to be

really nice to have this in Kafka you

know they actually contemplated the idea

to contribute they for cka add the

logging and and then put it back but

even that will not give them the file

system you know timing CU you don't have

access to that low low level thing

you um do

you yeah because you're just making a

Boop you're making a OPP posex right and

God knows the colonel does billions of

things before it does your actual ride

right all right guys that's what I

wanted to talk about uh yeah we're back

uh sorry about uh the Hiatus uh I've

been working as you might know my

operating system course and it's been

taking a long time with family and all

that stuff but yeah hopefully we'll came

back to the Cadence uh producing more

content and thank you so much and check

out the course it's os. husin dancer.com

fundamentals of operating systems it

took two years to build from all the

building the content and actually going

through all this stuff see you on the

next one


