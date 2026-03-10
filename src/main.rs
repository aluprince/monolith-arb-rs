fn main() {
    println!("Hello, world!");
    greet("JAY".to_string());
    let _g: () = greet("Prince".to_string());
}

fn greet(name: String) -> (){
    println!("Welcome {}", name);
}
